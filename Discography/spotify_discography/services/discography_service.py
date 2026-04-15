"""
discography_service.py
======================
Changements :
  - [0]  Fix artistes exclus : un artiste suivi ne peut jamais finir dans
         excluded_artists, même s'il ne figure pas sur une piste d'un album
         appears_on.  La liste excluded_artists ne contient désormais que des
         artistes qui NE SONT PAS dans followed_ids.
  - [9]  Cache temporel : si last_checked d'un album est plus récent que
         SCAN_INTERVAL, l'album est ignoré même si total_tracks n'a pas changé
         (économie d'appels API ~90% sur des runs répétés).
         Exception : FULL_RESYNC_MODE force toujours le scan complet.
"""

import time
import logging

from ..api.client import SpotifyClient
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
        self._client      = client
        self._state       = state_repo
        self._excluded    = excluded_repo
        # [0] Ensemble des IDs d'artistes que l'utilisateur suit.
        # Passé depuis main.py pour éviter d'exclure ses propres artistes.
        self._followed_ids: set = followed_ids or set()

    # ── API publique ──────────────────────────────────────────────────────────

    def get_new_tracks_for_artist(self, artist_id: str, artist_name: str) -> set:
        cached_albums  = self._state.get_artist_albums(artist_id)
        spotify_albums = self._fetch_artist_albums(artist_id)

        new_tracks = set()
        scanned    = 0
        skipped    = 0
        cached_hit = 0   # [9] albums ignorés grâce au cache temporel

        now = time.time()

        for album_id, album_data in spotify_albums.items():
            spotify_total = album_data["total_tracks"]
            album_name    = album_data["album_name"]
            cached        = cached_albums.get(album_id)

            # [9] Cache temporel : si last_checked est récent, on saute l'album.
            # On n'utilise le cache que si le nombre de tracks n'a pas changé
            # (un changement de total_tracks force toujours un rescan).
            if (
                not config.FULL_RESYNC_MODE
                and cached is not None
                and cached["total_tracks"] == spotify_total
                and (now - cached.get("last_checked", 0)) < config.SCAN_INTERVAL
            ):
                cached_hit += 1
                continue

            needs_scan = (
                config.FULL_RESYNC_MODE
                or cached is None
                or cached["total_tracks"] != spotify_total
                # Pas de cache temporel dans les cas ci-dessus — déjà filtré avant
            )

            if not needs_scan:
                # Cas : last_checked dépassé mais total_tracks inchangé
                # → on rescanne quand même (découverte de nouvelles tracks dans l'album)
                reason = f"cache expire ({int((now - cached.get('last_checked', 0)) / 3600)}h)"
            elif not cached:
                reason = "nouveau"
            else:
                reason = f"total_tracks {cached['total_tracks']}->{spotify_total}"

            logger.info("SCAN album '%s' (%s)", album_name, reason)

            tracks      = self._fetch_album_tracks(album_id, album_data, artist_id)
            new_tracks |= tracks
            scanned    += 1

            self._state.upsert_album(artist_id, album_id, {
                "album_name":   album_name,
                "total_tracks": spotify_total,
                "last_checked": now,
            })

        logger.info(
            "Artiste '%s' : albums scannes=%d ignores=%d cache_hit=%d nouvelles tracks=%d",
            artist_name, scanned, skipped, cached_hit, len(new_tracks)
        )
        return new_tracks

    # ── Fetch albums ──────────────────────────────────────────────────────────

    def _fetch_artist_albums(self, artist_id: str) -> dict:
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

    # ── Fetch tracks ──────────────────────────────────────────────────────────

    def _fetch_album_tracks(self, album_id: str, album_data: dict, artist_id: str) -> set:
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
                # [0] N'exclure QUE les artistes qui ne sont pas suivis.
                # Un artiste suivi qui partage un album appears_on avec
                # un autre artiste ne doit jamais finir dans excluded_artists.
                for aid, name in zip(track_artist_ids, track_artist_names):
                    if aid not in self._followed_ids:
                        self._excluded.add(name)

        logger.info(
            "Album '%s' : %d/%d tracks retenues",
            album_data["album_name"], len(tracks), album_data["total_tracks"]
        )
        return tracks
