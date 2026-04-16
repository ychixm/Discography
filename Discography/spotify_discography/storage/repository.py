"""
Interface abstraite pour le stockage.
Permet de switcher SQLite -> PostgreSQL ou autre sans toucher aux services.
"""
from abc import ABC, abstractmethod
from typing import Optional


class StateRepository(ABC):

    # ── Artists ───────────────────────────────────────────────────────────────

    @abstractmethod
    def get_artist(self, artist_id: str) -> Optional[dict]: ...

    @abstractmethod
    def upsert_artist(self, artist_id: str, data: dict) -> None: ...

    @abstractmethod
    def get_all_artists(self) -> list: ...

    @abstractmethod
    def get_ordered_artist_ids(self, followed_ids: set) -> list:
        """
        Retourne la liste ordonnée des artist_id à traiter dans ce cycle :
          1. Artistes avec album_checkpoint actif (reprise prioritaire)
          2. Artistes suivis jamais traités (last_scan == 0)
          3. Artistes suivis triés par last_scan ASC
        """
        ...

    @abstractmethod
    def all_followed_scanned(self, followed_ids: set) -> bool:
        """
        Retourne True si tous les artistes suivis ont last_scan > 0
        et qu'aucun album_checkpoint n'est actif.
        Condition pour déclencher le refresh de la liste des suivis.
        """
        ...

    # ── Albums ────────────────────────────────────────────────────────────────

    @abstractmethod
    def get_artist_albums(self, artist_id: str) -> dict: ...

    @abstractmethod
    def upsert_album(self, artist_id: str, album_id: str, data: dict) -> None: ...

    # ── Album checkpoint ──────────────────────────────────────────────────────

    @abstractmethod
    def save_album_checkpoint(
        self,
        artist_id: str,
        album_ids: list,
        last_album_idx: int,
    ) -> None:
        """
        Crée ou met à jour le checkpoint album pour un artiste.
        album_ids      : liste ordonnée des album_id récupérés depuis Spotify.
        last_album_idx : index du dernier album complètement traité (-1 si aucun).
        """
        ...

    @abstractmethod
    def load_album_checkpoint(self, artist_id: str) -> Optional[dict]:
        """
        Retourne { "album_ids": [...], "last_album_idx": int } ou None.
        """
        ...

    @abstractmethod
    def clear_album_checkpoint(self, artist_id: str) -> None:
        """Supprime le checkpoint album une fois l'artiste entièrement traité."""
        ...

    # ── Playlist tracks ───────────────────────────────────────────────────────

    @abstractmethod
    def get_playlist_tracks(self, artist_id: str) -> set: ...

    @abstractmethod
    def set_playlist_tracks(self, artist_id: str, track_ids: set) -> None: ...

    @abstractmethod
    def add_playlist_tracks(self, artist_id: str, track_ids: set) -> None: ...

    # ── Daemon state ──────────────────────────────────────────────────────────

    @abstractmethod
    def load_daemon_state(self) -> dict: ...

    @abstractmethod
    def save_daemon_state(
        self,
        last_followed_refresh: float,
        cycle_started_at: float,
        cycle_artist_idx: int,
    ) -> None: ...

    @abstractmethod
    def update_daemon_artist_idx(self, cycle_artist_idx: int) -> None: ...

    # ── Unavailable tracks ────────────────────────────────────────────────────

    @abstractmethod
    def add_unavailable_tracks(self, artist_id: str, track_ids: set) -> None: ...

    @abstractmethod
    def get_unavailable_tracks(self, artist_id: str) -> set: ...

    @abstractmethod
    def get_all_unavailable_tracks(self) -> list: ...

    # ── Removed tracks ────────────────────────────────────────────────────────

    @abstractmethod
    def add_removed_tracks(self, artist_id: str, track_ids: set) -> None: ...

    @abstractmethod
    def get_removed_tracks(self, artist_id: str) -> dict: ...

    @abstractmethod
    def get_non_blacklisted_removed_tracks(self, artist_id: str) -> set: ...

    @abstractmethod
    def blacklist_track(self, artist_id: str, track_id: str) -> None: ...

    @abstractmethod
    def get_all_removed_tracks_blacklisted(self, artist_id: str) -> set: ...

    @abstractmethod
    def get_all_removed_tracks(self) -> list: ...

    # ── Rate limit intervals ──────────────────────────────────────────────────

    @abstractmethod
    def save_rate_limit_interval(
        self,
        start_ts: float,
        end_ts: float,
        ok_calls: int,
        fail_calls: int,
        retry_after: float,
        endpoint: str,
    ) -> None: ...

    @abstractmethod
    def get_rate_limit_intervals(self, limit: int = 100) -> list: ...

    @abstractmethod
    def get_rate_limit_stats(self) -> dict: ...

    @abstractmethod
    def close(self) -> None: ...


class ExcludedRepository(ABC):

    @abstractmethod
    def add(self, artist_name: str) -> None: ...

    @abstractmethod
    def get_all(self) -> set: ...

    @abstractmethod
    def close(self) -> None: ...
