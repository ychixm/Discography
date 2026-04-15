"""
main.py
=======
Point d'entrée principal — mode daemon permanent.

Logique de cycle :
  1. Au démarrage, charger l'état daemon depuis la DB (last_followed_refresh,
     cycle_started_at, cycle_artist_idx).
  2. Si followed_refresh_interval est écoulé (ou premier démarrage) →
     recharger la liste des artistes suivis depuis Spotify.
  3. Construire la liste ordonnée des artistes à traiter dans ce cycle,
     triée par last_scan ASC (le plus ancien d'abord).
  4. Traiter chaque artiste. Après chaque artiste : sauvegarder le checkpoint
     ET l'état daemon (cycle_artist_idx).
  5. Sur RateLimitError :
       a. Sauvegarder le checkpoint et l'état daemon immédiatement.
       b. Mettre le statut en "rate_limited" sur le dashboard.
       c. Attendre le Retry-After.
       d. Reprendre exactement au même artiste.
  6. Sur _stop_event : sauvegarder et quitter proprement.
  7. Fin de cycle → attendre CYCLE_MIN_INTERVAL, puis recommencer
     depuis l'étape 2 (sans nécessairement recharger les artistes suivis).
"""

import time
import logging
import threading
import sys
import os

from . import config
from .api.client import SpotifyClient, RateLimitError
from .auth_flow import ensure_authenticated, save_tokens, check_encryption_warning
from .config_validator import validate as validate_config, ConfigurationError
from .services.discography_service import DiscographyService
from .services.playlist_service import PlaylistService
from .storage.state_repository import SQLiteStateRepository
from .storage.excluded_repository import SQLiteExcludedRepository
from . import dashboard_server
from . import tray_icon

########################################
# LOGGING
########################################
logger = logging.getLogger("spotify_discography")
logger.setLevel(logging.DEBUG if config.VERBOSE_LOGGING else logging.INFO)

_fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

fh = logging.FileHandler(config.LOG_FILE_PATH, encoding="utf-8")
fh.setFormatter(_fmt)
logger.addHandler(fh)

if sys.stderr and sys.stderr.isatty():
    ch = logging.StreamHandler()
    ch.setFormatter(_fmt)
    logger.addHandler(ch)


class _DashboardLogHandler(logging.Handler):
    LEVEL_MAP = {
        logging.DEBUG:    "INFO",
        logging.INFO:     "INFO",
        logging.WARNING:  "WARN",
        logging.ERROR:    "ERROR",
        logging.CRITICAL: "ERROR",
    }

    def emit(self, record: logging.LogRecord):
        level = self.LEVEL_MAP.get(record.levelno, "INFO")
        raw   = record.getMessage()
        if record.levelno == logging.INFO and any(
            kw in raw for kw in ("termine", "TERMINE", "ajoutees et cachees", "Nouvelle playlist")
        ):
            level = "OK"
        dashboard_server.push_log(level, raw)


_dash_handler = _DashboardLogHandler()
_dash_handler.setLevel(logging.DEBUG)
logger.addHandler(_dash_handler)

########################################
# ÉTAT GLOBAL
########################################
_tray:       tray_icon.TrayIcon | None = None
_stop_event: threading.Event           = threading.Event()


def _request_quit():
    logger.info("Arret demande depuis l'icone tray")
    _stop_event.set()


########################################
# DAEMON WORKER
########################################
def _daemon_worker(port: int):
    """
    Boucle principale du daemon.
    Tourne indéfiniment jusqu'à ce que _stop_event soit levé.
    """
    global _tray

    # ── Validation config ──────────────────────────────────────────────────
    config_path = os.environ.get("SPOTIFY_CONFIG_PATH", config._CONFIG_PATH)
    try:
        validate_config(config_path)
    except ConfigurationError as e:
        logger.error("CONFIGURATION INVALIDE :\n%s", e)
        if _tray:
            _tray.set_status("Erreur de configuration")
            _tray.notify("Configuration invalide", str(e)[:200])
        dashboard_server.update_run_state(status="stopped")
        return

    notify_fn = _tray.notify if _tray else None
    check_encryption_warning(notify_callback=notify_fn)

    # ── Authentification initiale ──────────────────────────────────────────
    if _tray:
        _tray.set_status("Authentification…")
        _tray.set_running(False)

    try:
        access_token, refresh_token, expires_at = ensure_authenticated()
    except RuntimeError as e:
        logger.error("Echec de l'authentification : %s", e)
        if _tray:
            _tray.set_status("Erreur d'authentification")
            _tray.notify("Erreur d'authentification", str(e))
        return

    client = SpotifyClient(
        access_token=access_token,
        refresh_token=refresh_token,
        expires_at=expires_at,
    )

    from .api.auth import refresh_access_token as _raf

    def _patched_maybe_refresh():
        if time.time() > client.expires_at - 60:
            logger.info("TOKEN_REFRESH proactive")
            client.access_token, client.expires_at = _raf(client.refresh_token)
            save_tokens(client.access_token, client.refresh_token, client.expires_at)

    client._maybe_refresh = _patched_maybe_refresh

    me_data = client.get_me()
    me_id   = me_data["id"]
    logger.info("Connecte en tant que : %s", me_id)

    if _tray:
        _tray.set_status(f"Connecte : {me_id}")

    # ── Repos ──────────────────────────────────────────────────────────────
    state_repo    = SQLiteStateRepository(config.STATE_DB_PATH)
    excluded_repo = SQLiteExcludedRepository(config.STATE_DB_PATH)

    # ── Chargement état daemon ──────────────────────────────────────────────
    daemon_state = state_repo.load_daemon_state()
    last_followed_refresh = daemon_state["last_followed_refresh"]

    # Cache mémoire des artistes suivis (rechargé selon l'intervalle)
    followed_map: dict = {}

    # ── Boucle daemon ──────────────────────────────────────────────────────
    cycle_num = 0

    while not _stop_event.is_set():
        cycle_num += 1
        cycle_start = time.time()

        # ── Rechargement de la liste des artistes suivis ───────────────────
        need_refresh = (
            not followed_map
            or (time.time() - last_followed_refresh) >= config.FOLLOWED_REFRESH_INTERVAL
        )

        if need_refresh:
            logger.info(
                "CYCLE %d — rechargement des artistes suivis (dernier il y a %.1fh)",
                cycle_num,
                (time.time() - last_followed_refresh) / 3600 if last_followed_refresh else float("inf"),
            )
            if _tray:
                _tray.set_status("Rechargement artistes suivis…")
            try:
                followed = client.get_followed_artists()
                followed_map = {a["id"]: a for a in followed}
                last_followed_refresh = time.time()
                state_repo.save_daemon_state(
                    last_followed_refresh=last_followed_refresh,
                    cycle_started_at=cycle_start,
                    cycle_artist_idx=0,
                )
                logger.info("CYCLE %d — %d artistes suivis", cycle_num, len(followed_map))
            except RateLimitError as e:
                _handle_rate_limit(e, state_repo, client, cycle_start, 0)
                continue
                _stop_event.wait(60)
                continue
        else:
            logger.info(
                "CYCLE %d — artistes en cache (%d), prochain refresh dans %.1fh",
                cycle_num,
                len(followed_map),
                (config.FOLLOWED_REFRESH_INTERVAL - (time.time() - last_followed_refresh)) / 3600,
            )

        followed_ids = set(followed_map.keys())

        # ── Construction des services ──────────────────────────────────────
        discography_svc = DiscographyService(
            client, state_repo, excluded_repo, followed_ids=followed_ids
        )
        playlist_svc = PlaylistService(client, state_repo, me_id)

        try:
            playlist_svc.load_existing_playlists()
        except RateLimitError as e:
            _handle_rate_limit(e, state_repo, client, cycle_start, 0)
            continue
        # Artistes connus en DB + nouveaux artistes suivis, triés par last_scan ASC
        known_artists = {a["artist_id"]: a for a in state_repo.get_artists_ordered_by_scan()}

        # On conserve l'ordre par last_scan pour les artistes connus
        # et on ajoute les nouveaux à la fin (last_scan = 0 → ils passent en premier)
        ordered_ids = list(known_artists.keys())
        for aid in followed_map:
            if aid not in known_artists:
                ordered_ids.append(aid)

        if config.MAX_ARTISTS_PER_RUN > 0:
            ordered_ids = ordered_ids[:config.MAX_ARTISTS_PER_RUN]

        total = len(ordered_ids)

        # ── Reprise sur interruption / 429 ────────────────────────────────
        start_idx = 0
        checkpoint = state_repo.load_checkpoint()
        if checkpoint and not config.FULL_RESYNC_MODE:
            age = time.time() - checkpoint["run_started_at"]
            if age < 7 * 24 * 3600:
                candidate = int(checkpoint["last_artist_idx"])
                if 0 < candidate < total:
                    start_idx = candidate
                    logger.info(
                        "CYCLE %d — reprise sur checkpoint : artiste %d/%d",
                        cycle_num, start_idx + 1, total,
                    )

        logger.info(
            "CYCLE %d — %d artistes à traiter (depuis idx=%d)",
            cycle_num, total, start_idx,
        )

        dashboard_server.update_run_state(
            status="running",
            run_start=cycle_start,
            total_artists=total,
            current_idx=start_idx,
            config={
                "market":                config.MARKET,
                "include_groups":        config.INCLUDE_GROUPS,
                "rate_limit_max_calls":  config.MAX_CALLS,
                "rate_limit_window":     config.RATE_LIMIT_WINDOW,
                "retry_max":             config.RETRY_MAX_ATTEMPTS,
                "full_resync":           config.FULL_RESYNC_MODE,
                "delay_between_artists": config.DELAY_BETWEEN_ARTISTS,
                "max_artists_per_run":   config.MAX_ARTISTS_PER_RUN,
                "followed_refresh_interval": config.FOLLOWED_REFRESH_INTERVAL,
                "cycle_min_interval":    config.CYCLE_MIN_INTERVAL,
            },
        )

        artists_run = [
            {
                "id":     aid,
                "name":   followed_map.get(aid, {}).get(
                              "name",
                              known_artists.get(aid, {}).get("artist_name", aid)
                          ),
                "status": "done" if i < start_idx else "",
                "tracks": 0,
            }
            for i, aid in enumerate(ordered_ids)
        ]

        stats = {
            "artists_processed": start_idx,
            "artists_skipped":   0,
            "tracks_added":      0,
        }

        if _tray:
            _tray.set_status(f"Cycle {cycle_num} — {start_idx}/{total}")
            _tray.set_running(True)

        # ── Boucle artistes ────────────────────────────────────────────────
        rate_limited = False

        for idx in range(start_idx, len(ordered_ids)):
            if _stop_event.is_set():
                logger.info("CYCLE %d — arrêt demandé à l'artiste %d/%d", cycle_num, idx + 1, total)
                state_repo.save_checkpoint(idx, cycle_start)
                state_repo.save_daemon_state(last_followed_refresh, cycle_start, idx)
                break

            artist_id   = ordered_ids[idx]
            artist      = followed_map.get(artist_id)
            artist_name = (
                artist["name"] if artist
                else known_artists.get(artist_id, {}).get("artist_name", artist_id)
            )

            if not artist:
                # Artiste non suivi — présent en DB mais plus dans followed
                stats["artists_skipped"] += 1
                _update_artist_status(artists_run, artist_id, "error", 0)
                dashboard_server.update_run_state(
                    current_idx=idx + 1,
                    stats={"artists_skipped": stats["artists_skipped"]},
                )
                continue

            playlist_name = f"{artist_name} - Discography"

            logger.info(
                "===== CYCLE %d · Artiste %d/%d : %s =====",
                cycle_num, idx + 1, total, artist_name,
            )

            _update_artist_status(artists_run, artist_id, "scanning", 0)
            dashboard_server.update_run_state(
                current_artist=artist_name,
                current_idx=idx,
                artists_run=artists_run,
            )

            if _tray:
                _tray.set_status(f"Cycle {cycle_num} · {idx+1}/{total} — {artist_name}")

            artist_start = time.time()

            try:
                playlist_id = playlist_svc.get_or_create_playlist(artist_id, playlist_name)

                state_repo.upsert_artist(artist_id, {
                    "artist_name": artist_name,
                    "playlist_id": playlist_id,
                    "last_scan":   known_artists.get(artist_id, {}).get("last_scan", 0.0),
                })

                new_tracks = discography_svc.get_new_tracks_for_artist(artist_id, artist_name)
                added      = playlist_svc.add_tracks(playlist_id, artist_id, new_tracks)

                stats["tracks_added"]      += added
                stats["artists_processed"] += 1

                state_repo.upsert_artist(artist_id, {
                    "artist_name": artist_name,
                    "playlist_id": playlist_id,
                    "last_scan":   time.time(),
                })

                # Checkpoint après chaque artiste traité avec succès
                state_repo.save_checkpoint(idx + 1, cycle_start)
                state_repo.update_daemon_artist_idx(idx + 1)

                _update_artist_status(artists_run, artist_id, "done", added)
                logger.info(
                    "Artiste '%s' terminé en %.1fs | %d tracks ajoutées",
                    artist_name, time.time() - artist_start, added,
                )

            except RateLimitError as e:
                # ── Sauvegarde immédiate puis attente ──────────────────────
                logger.warning(
                    "429 sur artiste '%s' — sauvegarde checkpoint idx=%d, attente %.1fs",
                    artist_name, idx, e.retry_after,
                )
                state_repo.save_checkpoint(idx, cycle_start)
                state_repo.save_daemon_state(last_followed_refresh, cycle_start, idx)

                # Persister l'intervalle clos dans la DB
                ci = client._current_interval
                state_repo.save_rate_limit_interval(
                    start_ts=ci["start_ts"],
                    end_ts=ci["end_ts"] or time.time(),
                    ok_calls=ci["ok_calls"],
                    fail_calls=ci["fail_calls"],
                    retry_after=e.retry_after,
                    endpoint=ci.get("endpoint") or "",
                )

                dashboard_server.update_run_state(
                    status="rate_limited",
                    current_artist=artist_name,
                )
                if _tray:
                    _tray.set_running(False)
                    _tray.set_status(f"Rate limit — attente {e.retry_after:.0f}s…")
                    _tray.notify(
                        "Rate limit Spotify",
                        f"Attente {e.retry_after:.0f}s — reprise automatique",
                    )

                # Attente interruptible
                _stop_event.wait(timeout=e.retry_after)

                if _stop_event.is_set():
                    break

                logger.info("Fin d'attente 429 — reprise du cycle %d à l'artiste %d", cycle_num, idx + 1)
                # Ouvrir un nouvel intervalle dès la reprise
                client.notify_retry_after_elapsed()
                dashboard_server.update_run_state(status="running")
                if _tray:
                    _tray.set_running(True)

                # On reprend CET artiste (idx inchangé)
                rate_limited = True
                start_idx = idx
                break   # sort de la boucle for → recommence le cycle while

            except Exception as e:
                _update_artist_status(artists_run, artist_id, "error", 0)
                logger.error(
                    "ERREUR artiste '%s' (%s) : %s",
                    artist_name, artist_id, e, exc_info=True,
                )

            dashboard_server.update_run_state(
                current_idx=idx + 1,
                artists_run=artists_run,
                stats={
                    "artists_processed": stats["artists_processed"],
                    "artists_skipped":   stats["artists_skipped"],
                    "tracks_added":      stats["tracks_added"],
                    "api_total_calls":   client.total_calls,
                    "api_429":           client.stats.get("429", 0),
                    "api_5xx":           client.stats.get("5xx", 0),
                    "api_detail":        dict(client.stats),
                },
            )

            time.sleep(config.DELAY_BETWEEN_ARTISTS)

        # ── Si rate_limited → reprendre le cycle sans attente ─────────────
        if rate_limited and not _stop_event.is_set():
            logger.info("Reprise du cycle %d après 429 depuis idx=%d", cycle_num, start_idx)
            continue   # repart au début du while avec start_idx sauvegardé en DB

        if _stop_event.is_set():
            break

        # ── Fin de cycle normale ───────────────────────────────────────────
        cycle_duration = time.time() - cycle_start
        logger.info(
            "CYCLE %d TERMINÉ — %d artistes traités, %d tracks ajoutées, durée %.1fs",
            cycle_num,
            stats["artists_processed"],
            stats["tracks_added"],
            cycle_duration,
        )

        state_repo.clear_checkpoint()
        state_repo.save_daemon_state(
            last_followed_refresh=last_followed_refresh,
            cycle_started_at=0.0,
            cycle_artist_idx=0,
        )

        dashboard_server.update_run_state(
            status="idle",
            current_artist=None,
            current_idx=total,
            stats={
                "artists_processed": stats["artists_processed"],
                "artists_skipped":   stats["artists_skipped"],
                "tracks_added":      stats["tracks_added"],
                "api_total_calls":   client.total_calls,
                "api_detail":        dict(client.stats),
            },
        )

        if _tray:
            _tray.set_running(False)
            _tray.set_status(
                f"Cycle {cycle_num} terminé — "
                f"{stats['artists_processed']} artistes, {stats['tracks_added']} tracks"
            )
            _tray.notify(
                f"Cycle {cycle_num} terminé",
                f"{stats['artists_processed']} artistes · {stats['tracks_added']} tracks ajoutées",
            )

        # Attente avant le prochain cycle
        wait_next = max(0, config.CYCLE_MIN_INTERVAL)
        logger.info(
            "Prochain cycle dans %.1f min (CYCLE_MIN_INTERVAL=%ds)",
            wait_next / 60, wait_next,
        )
        if _tray:
            _tray.set_status(f"Idle — prochain cycle dans {wait_next // 60}min")

        _stop_event.wait(timeout=wait_next)

    # ── Arrêt propre ──────────────────────────────────────────────────────
    logger.info("Daemon arrêté proprement.")
    state_repo.close()
    excluded_repo.close()


########################################
# HELPERS
########################################
def _handle_rate_limit(
    e: "RateLimitError",
    state_repo: SQLiteStateRepository,
    client: "SpotifyClient",
    cycle_start: float,
    idx: int,
):
    """Sauvegarde et attend sur un 429 survenu hors boucle artistes."""
    logger.warning("429 hors boucle artistes — attente %.1fs", e.retry_after)
    state_repo.save_checkpoint(idx, cycle_start)

    # Persister l'intervalle clos
    ci = client._current_interval
    state_repo.save_rate_limit_interval(
        start_ts=ci["start_ts"],
        end_ts=ci["end_ts"] or time.time(),
        ok_calls=ci["ok_calls"],
        fail_calls=ci["fail_calls"],
        retry_after=e.retry_after,
        endpoint=ci.get("endpoint") or "",
    )

    dashboard_server.update_run_state(status="rate_limited")
    if _tray:
        _tray.set_running(False)
        _tray.set_status(f"Rate limit — attente {e.retry_after:.0f}s…")
    _stop_event.wait(timeout=e.retry_after)
    client.notify_retry_after_elapsed()
    dashboard_server.update_run_state(status="running")
    if _tray:
        _tray.set_running(True)


def _update_artist_status(artists_run: list, artist_id: str, status: str, tracks: int):
    for a in artists_run:
        if a["id"] == artist_id:
            a["status"] = status
            if tracks:
                a["tracks"] = tracks
            break


########################################
# MAIN
########################################
def main():
    global _tray

    port = dashboard_server.start()
    if port:
        logger.info("Dashboard sur http://127.0.0.1:%d", port)
    else:
        logger.warning("Dashboard désactivé (aucun port disponible).")

    _tray = tray_icon.create(port or 8080, quit_callback=_request_quit)

    if not tray_icon.is_available():
        logger.warning("Icône tray non disponible — installez : pip install pystray pillow")

    worker = threading.Thread(
        target=_daemon_worker, args=(port,), daemon=True, name="spotify-daemon"
    )
    worker.start()

    try:
        _tray.run()
    except KeyboardInterrupt:
        _request_quit()

    _stop_event.set()
    dashboard_server.stop()


if __name__ == "__main__":
    main()
