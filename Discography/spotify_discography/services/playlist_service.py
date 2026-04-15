import logging

from ..api.client import SpotifyClient
from ..storage.repository import StateRepository
from .. import config

logger = logging.getLogger("spotify_discography")


class PlaylistService:

    def __init__(self, client: SpotifyClient, repo: StateRepository, me_id: str):
        self._client     = client
        self._repo       = repo
        self._me_id      = me_id
        self._name_to_id: dict = {}

    def load_existing_playlists(self):
        self._name_to_id = self._client.load_all_playlists(self._me_id)

        # Synchronise la DB avec les playlists trouvées sur Spotify,
        # pour éviter les doublons si la DB ne contient pas encore le playlist_id.
        for artist in self._repo.get_all_artists():
            artist_id     = artist["artist_id"]
            artist_name   = artist["artist_name"]
            playlist_name = f"{artist_name} - Discography"
            if playlist_name in self._name_to_id:
                pid = self._name_to_id[playlist_name]
                if artist.get("playlist_id") != pid:
                    self._repo.upsert_artist(artist_id, {
                        "artist_name": artist_name,
                        "playlist_id": pid,
                        "last_scan":   artist.get("last_scan", 0.0),
                    })
                    logger.info(
                        "Sync DB playlist '%s' (%s) → %s",
                        playlist_name, artist_id, pid,
                    )

    def get_or_create_playlist(self, artist_id: str, playlist_name: str) -> str:
        if playlist_name in self._name_to_id:
            pid = self._name_to_id[playlist_name]
            logger.info("Playlist cache RAM : '%s' (%s)", playlist_name, pid)
            return pid

        artist_data = self._repo.get_artist(artist_id)
        if artist_data and artist_data.get("playlist_id"):
            pid = artist_data["playlist_id"]
            logger.info("Playlist cache DB : '%s' (%s)", playlist_name, pid)
            self._name_to_id[playlist_name] = pid
            return pid

        pid = self._create_playlist(playlist_name)
        self._name_to_id[playlist_name] = pid
        return pid

    def _create_playlist(self, name: str) -> str:
        r   = self._client.post(
            f"{config.API_BASE}/me/playlists",
            json={"name": name, "public": True},
        )
        pid = r.json()["id"]
        logger.info("Nouvelle playlist : '%s' (%s)", name, pid)
        return pid

    def add_tracks(self, playlist_id: str, artist_id: str, track_ids: set) -> int:
        cached     = self._repo.get_playlist_tracks(artist_id)
        new_tracks = list(track_ids - cached)

        if not new_tracks:
            logger.info("Aucune nouvelle track pour %s", artist_id)
            return 0

        for i in range(0, len(new_tracks), 100):
            batch = new_tracks[i:i + 100]
            uris  = [f"spotify:track:{t}" for t in batch]
            self._client.post(
                f"{config.API_BASE}/playlists/{playlist_id}/items",
                json={"uris": uris},
            )
            logger.info("Batch %d : %d tracks ajoutees", i // 100 + 1, len(batch))

        self._repo.add_playlist_tracks(artist_id, set(new_tracks))
        logger.info("%d tracks ajoutees et cachees pour %s", len(new_tracks), artist_id)
        return len(new_tracks)

    def force_resync_playlist_tracks(self, playlist_id: str, artist_id: str) -> set:
        logger.info("FORCE_RESYNC playlist %s pour %s", playlist_id, artist_id)
        items = self._client.paginate(
            f"{config.API_BASE}/playlists/{playlist_id}/items",
            params={"limit": config.LIMIT_PLAYLIST_ITEMS},
        )
        track_ids = {
            it["track"]["id"]
            for it in items
            if it.get("track") and it["track"] and it["track"].get("id")
        }
        self._repo.set_playlist_tracks(artist_id, track_ids)
        logger.info("Resync : %d tracks cachees pour %s", len(track_ids), artist_id)
        return track_ids
