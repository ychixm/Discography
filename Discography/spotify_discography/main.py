"""
main.py
=======
Point d'entrée principal — mode daemon permanent.

Logique de cycle :
  1. Tous les artistes suivis ont été traités (all_followed_scanned) ?
     → OUI : GET /me/following → mise à jour followed_map
     → NON : on garde followed_map en mémoire

  2. Construire la liste ordonnée via get_ordered_artist_ids() :
       a. Artistes avec album_checkpoint actif (reprise prioritaire)
       b. Artistes jamais traités (last_scan == 0)
       c. Artistes triés par last_scan ASC

  3. Pour chaque artiste :
       a. get_new_tracks_for_artist() → accumule les tracks (checkpoint album géré
          dans discography_service)
       b. add_tracks() → un seul appel Spotify en fin d'artiste
       c. upsert_artist(last_scan=now)
       d. clear_album_checkpoint()

  4. Sur RateLimitError :
       a. Sauvegarde daemon_state + rate_limit_interval
       b. Statut "rate_limited" sur le dashboard
       c. Attente Retry-After (interruptible)
       d. Reprise : l'artiste interrompu est prioritaire via album_checkpoint

  5. Fin de cycle → retour en 1 sans attente fixe
     (le refresh followed remplace CYCLE_MIN_INTERVAL comme garde-fou naturel)
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

    # ── Authentification ───────────────────────────────────────────────────
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

    # ── Cache mémoire des artistes suivis ──────────────────────────────────
    # Chargé une première fois au démarrage, puis rafraîchi uniquement quand
    # tous les artistes ont été traités (all_followed_scanned).
    followed_map: dict = {}

    # ── Boucle daemon ──────────────────────────────────────────────────────
    cycle_num = 0

    while not _stop_event.is_set():
        cycle_num  += 1
        cycle_start = time.time()

        # ── 1. Refresh de la liste des suivis si nécessaire ────────────────
        followed_ids = set(followed_map.keys())

        if not followed_map or state_repo.all_followed_scanned(followed_ids):
            logger.info(
                "CYCLE %d — refresh liste des artistes suivis", cycle_num
            )
            if _tray:
                _tray.set_status("Rechargement artistes suivis…")
            try:
                followed      = client.get_followed_artists()
                followed_map  = {a["id"]: a for a in followed}
                followed_ids  = set(followed_map.keys())
                logger.info(
                    "CYCLE %d — %d artistes suivis", cycle_num, len(followed_map)
                )
            except RateLimitError as e:
                _handle_rate_limit(e, state_repo, client, cycle_start, 0)
                if _stop_event.is_set():
                    break
                continue
        else:
            logger.info(
                "CYCLE %d — artistes en cache (%d), pas encore tous traités",
                cycle_num, len(followed_map),
            )

        # ── 2. Construction de la liste ordonnée ───────────────────────────
        ordered_ids = state_repo.get_ordered_artist_ids(followed_ids)

        if config.MAX_ARTISTS_PER_RUN > 0:
            ordered_ids = ordered_ids[:config.MAX_ARTISTS_PER_RUN]

        total = len(ordered_ids)

        if total == 0:
            logger.info("CYCLE %d — aucun artiste à traiter, attente", cycle_num)
            _stop_event.wait(timeout=config.CYCLE_MIN_INTERVAL)
            continue

        # ── Services ───────────────────────────────────────────────────────
        discography_svc = DiscographyService(
            client, state_repo, excluded_repo, followed_ids=followed_ids
        )
        playlist_svc = PlaylistService(client, state_repo, me_id)

        try:
            playlist_svc.load_existing_playlists()
        except RateLimitError as e:
            _handle_rate_limit(e, state_repo, client, cycle_start, 0)
            if _stop_event.is_set():
                break
            continue

        # ── Dashboard ──────────────────────────────────────────────────────
        dashboard_server.update_run_state(
            status="running",
            run_start=cycle_start,
            total_artists=total,
            current_idx=0,
            config={
                "market":                config.MARKET,
                "include_groups":        config.INCLUDE_GROUPS,
                "rate_limit_max_calls":  config.MAX_CALLS,
                "rate_limit_window":     config.RATE_LIMIT_WINDOW,
                "retry_max":             config.RETRY_MAX_ATTEMPTS,
                "full_resync":           config.FULL_RESYNC_MODE,
                "delay_between_artists": config.DELAY_BETWEEN_ARTISTS,
                "max_artists_per_run":   config.MAX_ARTISTS_PER_RUN,
                "cycle_min_interval":    config.CYCLE_MIN_INTERVAL,
            },
            daemon_meta={
                "cycle_num":     cycle_num,
                "followed_count": len(followed_map),
            },
        )

        artists_run = []
        for aid in ordered_ids:
            name_from_followed = followed_map.get(aid, {}).get("name")
            if not name_from_followed:
                db = state_repo.get_artist(aid)
                name_from_followed = db.get("artist_name", aid) if db else aid
            artists_run.append({
                "id":     aid,
                "name":   name_from_followed,
                "status": "",
                "tracks": 0,
            })

        stats = {
            "artists_processed": 0,
            "artists_skipped":   0,
            "tracks_added":      0,
        }

        if _tray:
            _tray.set_status(f"Cycle {cycle_num} — 0/{total}")
            _tray.set_running(True)

        # ── 3. Boucle artistes ─────────────────────────────────────────────
        rate_limited = False

        for idx, artist_id in enumerate(ordered_ids):
            if _stop_event.is_set():
                logger.info(
                    "CYCLE %d — arrêt demandé à l'artiste %d/%d",
                    cycle_num, idx + 1, total,
                )
                state_repo.save_daemon_state(0.0, cycle_start, idx)
                break

            artist      = followed_map.get(artist_id)
            artist_name = (
                artist["name"] if artist
                else (
                    state_repo.get_artist(artist_id) or {}
                ).get("artist_name", artist_id)
            )

            # Artiste désabonné mais checkpoint actif → on termine son scan
            # puis on passe (pas d'ajout à excluded, pas de playlist créée)
            is_followed = artist_id in followed_ids

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
                _tray.set_status(
                    f"Cycle {cycle_num} · {idx + 1}/{total} — {artist_name}"
                )

            artist_start = time.time()

            try:
                # ── a. Playlist ────────────────────────────────────────────
                # On charge l'état DB une seule fois pour cet artiste
                db_artist = state_repo.get_artist(artist_id)

                if is_followed:
                    playlist_id = playlist_svc.get_or_create_playlist(
                        artist_id, playlist_name
                    )
                    state_repo.upsert_artist(artist_id, {
                        "artist_name": artist_name,
                        "playlist_id": playlist_id,
                        "last_scan":   db_artist.get("last_scan", 0.0) if db_artist else 0.0,
                    })
                else:
                    # Artiste désabonné : récupère l'id depuis la DB
                    playlist_id = db_artist.get("playlist_id") if db_artist else None

                # ── b. Scan albums → accumulation tracks ───────────────────
                new_tracks = discography_svc.get_new_tracks_for_artist(
                    artist_id, artist_name
                )

                # ── c. Ajout à la playlist (un seul bloc) ──────────────────
                if playlist_id and new_tracks:
                    added = playlist_svc.add_tracks(
                        playlist_id, artist_id, new_tracks
                    )
                else:
                    added = 0
                    if new_tracks and not playlist_id:
                        logger.warning(
                            "Artiste '%s' : %d tracks trouvées mais pas de playlist",
                            artist_name, len(new_tracks),
                        )

                stats["tracks_added"]      += added
                stats["artists_processed"] += 1

                # ── d. Finalisation ────────────────────────────────────────
                state_repo.upsert_artist(artist_id, {
                    "artist_name": artist_name,
                    "playlist_id": playlist_id,
                    "last_scan":   time.time(),
                })
                state_repo.clear_album_checkpoint(artist_id)
                state_repo.update_daemon_artist_idx(idx + 1)

                _update_artist_status(artists_run, artist_id, "done", added)
                logger.info(
                    "Artiste '%s' terminé en %.1fs | %d tracks ajoutées",
                    artist_name, time.time() - artist_start, added,
                )

            except RateLimitError as e:
                # L'album_checkpoint est déjà à jour (géré dans discography_service)
                logger.warning(
                    "429 sur artiste '%s' — checkpoint idx=%d, attente %.1fs",
                    artist_name, idx, e.retry_after,
                )
                state_repo.save_daemon_state(0.0, cycle_start, idx)

                # Persiste l'intervalle rate limit
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
                    stats={
                        "retry_after_last": e.retry_after,
                    },
                )
                if _tray:
                    _tray.set_running(False)
                    _tray.set_status(f"Rate limit — attente {e.retry_after:.0f}s…")
                    _tray.notify(
                        "Rate limit Spotify",
                        f"Attente {e.retry_after:.0f}s — reprise automatique",
                    )

                _stop_event.wait(timeout=e.retry_after)

                if _stop_event.is_set():
                    break

                logger.info(
                    "Fin d'attente 429 — reprise cycle %d à l'artiste %d",
                    cycle_num, idx + 1,
                )
                client.notify_retry_after_elapsed()
                dashboard_server.update_run_state(status="running")
                if _tray:
                    _tray.set_running(True)

                # L'artiste interrompu sera prioritaire au prochain tour
                # grâce à son album_checkpoint actif dans get_ordered_artist_ids
                rate_limited = True
                break

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

        # ── Reprise après 429 : recommence le while sans attente ───────────
        if rate_limited and not _stop_event.is_set():
            logger.info(
                "Reprise du cycle %d après 429 — l'artiste interrompu "
                "est prioritaire via son album_checkpoint",
                cycle_num,
            )
            continue

        if _stop_event.is_set():
            break

        # ── 4. Fin de cycle normale ────────────────────────────────────────
        cycle_duration = time.time() - cycle_start
        logger.info(
            "CYCLE %d TERMINÉ — %d artistes traités, %d tracks ajoutées, "
            "durée %.1fs",
            cycle_num,
            stats["artists_processed"],
            stats["tracks_added"],
            cycle_duration,
        )

        state_repo.save_daemon_state(
            last_followed_refresh=time.time(),
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
            daemon_meta={
                "cycle_num":      cycle_num,
                "followed_count": len(followed_map),
            },
        )

        if _tray:
            _tray.set_running(False)
            _tray.set_status(
                f"Cycle {cycle_num} terminé — "
                f"{stats['artists_processed']} artistes, "
                f"{stats['tracks_added']} tracks"
            )
            _tray.notify(
                f"Cycle {cycle_num} terminé",
                f"{stats['artists_processed']} artistes · "
                f"{stats['tracks_added']} tracks ajoutées",
            )

        # Attente minimale entre deux cycles pour éviter un spin tight
        # quand tous les artistes sont récents (SCAN_INTERVAL pas encore écoulé)
        _stop_event.wait(timeout=config.CYCLE_MIN_INTERVAL)

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
        stats={"retry_after_last": e.retry_after},
    )
    if _tray:
        _tray.set_running(False)
        _tray.set_status(f"Rate limit — attente {e.retry_after:.0f}s…")

    _stop_event.wait(timeout=e.retry_after)
    client.notify_retry_after_elapsed()
    dashboard_server.update_run_state(status="running")

    if _tray:
        _tray.set_running(True)


def _update_artist_status(
    artists_run: list,
    artist_id: str,
    status: str,
    tracks: int,
):
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
        logger.warning(
            "Icône tray non disponible — installez : pip install pystray pillow"
        )

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
