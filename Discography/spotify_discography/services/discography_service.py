"""
discography_service.py
======================
Logique de scan des discographies.

Flux par artiste :
  1. Charger le checkpoint album depuis la DB (s'il existe).
     → OUI : réutilise album_ids sans appel Spotify, reprend à last_album_idx + 1
     → NON : _fetch_artist_albums() puis save_album_checkpoint(..., -1)
  2. Pour chaque album (depuis l'index de reprise) :
     - Skip si last_checked récent ET total_tracks inchangé (cache temporel)
     - _fetch_album_tracks() → accumule dans new_tracks (set en mémoire)
     - upsert_album() → persiste en DB
     - save_album_checkpoint(..., idx courant)
     Si RateLimitError → re-levée immédiatement ; le checkpoint est déjà à jour.
  3. Une fois tous les albums scannés → retourne new_tracks.
     C'est main.py qui appelle add_tracks() ensuite.

Règle d'exclusion :
  Un artiste présent dans followed_ids ne peut jamais être ajouté à
  excluded_artists, même s'il apparaît sur une piste d'un album appears_on
  d'un autre artiste.
"""

from __future__ import annotations

import time
import logging

from ..api.client import SpotifyClient, RateLimitError
from ..storage.repository import StateRepository, ExcludedRepository
from .. import config

logger = logging.getLogger("spotify_discography")


class DiscographyService:

    def __init__(
        self,
        client: SpotifyClient,
        state_repo: StateRepository,
        excluded_repo: ExcludedRepository,
        followed_ids: set | None = None,
    ):
        self._client       = client
        self._state        = state_repo
        self._excluded     = excluded_repo
        self._followed_ids: set = followed_ids or set()

    # ── API publique ──────────────────────────────────────────────────────────

    def get_new_tracks_for_artist(self, artist_id: str, artist_name: str) -> set:
        """
        Scanne la discographie d'un artiste et retourne le set de track_id
        à ajouter à sa playlist.

        Gestion du checkpoint album :
        - Si un checkpoint existe → on réutilise album_ids (pas d'appel Spotify)
          et on reprend à last_album_idx + 1.
        - Sinon → on fetch la liste des albums et on initialise le checkpoint.

        Le checkpoint est mis à jour après chaque album traité avec succès.
        En cas de RateLimitError, le checkpoint est déjà cohérent → on re-lève
        pour que main.py gère l'attente et le checkpoint artiste.
        """
        now = time.time()

        # ── Chargement ou initialisation du checkpoint album ──────────────
        checkpoint    = self._state.load_album_checkpoint(artist_id)
        # spotify_albums est peuplé uniquement sur un premier scan (pas de checkpoint).
        # En reprise, album_ids est relu depuis le checkpoint sans appel Spotify.
        spotify_albums: dict = {}

        if checkpoint is not None:
            album_ids  = checkpoint["album_ids"]
            start_idx  = checkpoint["last_album_idx"] + 1
            logger.info(
                "Reprise checkpoint album '%s' : %d albums, reprise à idx=%d",
                artist_name, len(album_ids), start_idx,
            )
        else:
            spotify_albums = self._fetch_artist_albums(artist_id)
            album_ids      = list(spotify_albums.keys())
            start_idx      = 0
            self._state.save_album_checkpoint(artist_id, album_ids, -1)
            logger.info(
                "Nouveau scan '%s' : %d albums trouvés",
                artist_name, len(album_ids),
            )

        # Cache des albums déjà en DB pour cet artiste
        cached_albums = self._state.get_artist_albums(artist_id)

        # Accumulateur en mémoire : tracks à ajouter à la playlist
        new_tracks: set = set()
        scanned    = 0
        skipped    = 0

        # ── Boucle albums ─────────────────────────────────────────────────
        for idx in range(start_idx, len(album_ids)):
            album_id = album_ids[idx]

            # Les métadonnées de l'album (name, total_tracks) viennent soit
            # du cache DB (reprise), soit du dict spotify_albums (premier scan).
            # On recharge depuis DB pour couvrir les deux cas.
            cached = cached_albums.get(album_id)

            # Résolution du nom et total_tracks :
            # - en reprise : on a uniquement album_id, les métadonnées sont en DB
            # - en premier scan : spotify_albums est disponible en closure
            #   mais on préfère la DB pour être cohérent dans les deux cas.
            if cached:
                album_name    = cached["album_name"]
                spotify_total = cached["total_tracks"]
                # Vérification cache temporel : si last_checked récent ET
                # total_tracks inchangé → skip sans appel Spotify
                if (
                    not config.FULL_RESYNC_MODE
                    and (now - cached.get("last_checked", 0)) < config.SCAN_INTERVAL
                ):
                    skipped += 1
                    # Le checkpoint avance quand même pour ne pas retraiter
                    self._state.save_album_checkpoint(artist_id, album_ids, idx)
                    continue
            else:
                # Album inconnu en DB.
                # Cas 1 : premier scan → spotify_albums est peuplé.
                # Cas 2 : reprise avec album absent de la DB (DB corrompue ou
                #         album ajouté entre deux runs) → fetch individuel.
                if spotify_albums:
                    meta = spotify_albums.get(album_id)
                else:
                    meta = self._fetch_single_album_meta(album_id)
                if meta is None:
                    logger.warning(
                        "Album %s introuvable sur Spotify, ignoré", album_id
                    )
                    self._state.save_album_checkpoint(artist_id, album_ids, idx)
                    continue
                album_name    = meta["album_name"]
                spotify_total = meta["total_tracks"]

            # ── Scan de l'album ───────────────────────────────────────────
            if cached and cached["total_tracks"] != spotify_total:
                reason = f"total_tracks {cached['total_tracks']}→{spotify_total}"
            elif not cached:
                reason = "nouveau"
            else:
                reason = f"cache expiré ({int((now - cached.get('last_checked', 0)) / 3600)}h)"

            logger.info("SCAN album '%s' (%s)", album_name, reason)

            # Peut lever RateLimitError → propagée telle quelle.
            # Le checkpoint est à last_album_idx = idx - 1 (ou -1) à ce stade,
            # ce qui est correct : on reprendra à idx au prochain run.
            tracks = self._fetch_album_tracks(album_id, album_name, artist_id)

            new_tracks |= tracks
            scanned    += 1

            # Persiste l'album et avance le checkpoint
            self._state.upsert_album(artist_id, album_id, {
                "album_name":   album_name,
                "total_tracks": spotify_total,
                "last_checked": now,
            })
            self._state.save_album_checkpoint(artist_id, album_ids, idx)

        logger.info(
            "Artiste '%s' : albums scannés=%d ignorés=%d nouvelles tracks=%d",
            artist_name, scanned, skipped, len(new_tracks),
        )
        return new_tracks

    # ── Fetch liste des albums ────────────────────────────────────────────────

    def _fetch_artist_albums(self, artist_id: str) -> dict:
        """
        Retourne { album_id: { album_name, total_tracks }, ... }
        dans l'ordre renvoyé par Spotify.
        """
        params = {
            "limit":          config.LIMIT_ALBUMS,
            "include_groups": config.INCLUDE_GROUPS,
        }
        if self._client.market:
            params["market"] = self._client.market

        items = self._client.paginate(
            f"{config.API_BASE}/artists/{artist_id}/albums",
            params=params,
        )
        albums = {}
        for a in items:
            if a and a.get("id"):
                albums[a["id"]] = {
                    "album_name":   a["name"],
                    "total_tracks": a["total_tracks"],
                }
        logger.info("Albums Spotify pour %s : %d", artist_id, len(albums))
        return albums

    def _fetch_single_album_meta(self, album_id: str) -> dict | None:
        """
        Fetch les métadonnées d'un album seul (cas de reprise avec album
        absent de la DB). Retourne None si l'album est introuvable.
        """
        try:
            r = self._client.get(f"{config.API_BASE}/albums/{album_id}")
            data = r.json()
            return {
                "album_name":   data["name"],
                "total_tracks": data["total_tracks"],
            }
        except Exception as e:
            logger.warning("_fetch_single_album_meta %s : %s", album_id, e)
            return None

    # ── Fetch pistes d'un album ───────────────────────────────────────────────

    def _fetch_album_tracks(
        self,
        album_id: str,
        album_name: str,
        artist_id: str,
    ) -> set:
        """
        Retourne le set des track_id appartenant à artist_id dans cet album.
        Les artistes tiers non suivis sont ajoutés à excluded_artists.
        """
        params = {"limit": config.LIMIT_ALBUM_TRACKS}
        if self._client.market:
            params["market"] = self._client.market

        items = self._client.paginate(
            f"{config.API_BASE}/albums/{album_id}/tracks",
            params=params,
        )
        tracks = set()
        for t in items:
            if not t or not t.get("id"):
                continue
            track_artist_ids   = {a["id"]   for a in t.get("artists", [])}
            track_artist_names = {a["name"] for a in t.get("artists", [])}

            if artist_id in track_artist_ids:
                tracks.add(t["id"])
            else:
                # N'exclure que les artistes non suivis
                for aid, name in zip(track_artist_ids, track_artist_names):
                    if aid not in self._followed_ids:
                        self._excluded.add(name)

        logger.info(
            "Album '%s' : %d tracks retenues",
            album_name, len(tracks),
        )
        return tracks
