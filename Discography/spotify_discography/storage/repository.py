"""
Interface abstraite pour le stockage.
Permet de switcher SQLite -> PostgreSQL ou autre sans toucher aux services.
"""
from abc import ABC, abstractmethod
from typing import Optional


class StateRepository(ABC):

    @abstractmethod
    def get_artist(self, artist_id: str) -> Optional[dict]: ...

    @abstractmethod
    def upsert_artist(self, artist_id: str, data: dict) -> None: ...

    @abstractmethod
    def get_all_artists(self) -> list: ...

    @abstractmethod
    def get_artist_albums(self, artist_id: str) -> dict: ...

    @abstractmethod
    def upsert_album(self, artist_id: str, album_id: str, data: dict) -> None: ...

    @abstractmethod
    def get_playlist_tracks(self, artist_id: str) -> set: ...

    @abstractmethod
    def set_playlist_tracks(self, artist_id: str, track_ids: set) -> None: ...

    @abstractmethod
    def add_playlist_tracks(self, artist_id: str, track_ids: set) -> None: ...

    # [1] Reprise sur interruption
    @abstractmethod
    def save_checkpoint(self, last_artist_idx: int, run_started_at: float) -> None: ...

    @abstractmethod
    def load_checkpoint(self) -> Optional[dict]: ...

    @abstractmethod
    def clear_checkpoint(self) -> None: ...

    # [2-A] Tracks indisponibles
    @abstractmethod
    def add_unavailable_tracks(self, artist_id: str, track_ids: set) -> None: ...

    @abstractmethod
    def get_unavailable_tracks(self, artist_id: str) -> set: ...

    @abstractmethod
    def get_all_unavailable_tracks(self) -> list: ...

    # [2-B] Tracks retirées manuellement
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

    @abstractmethod
    def close(self) -> None: ...


class ExcludedRepository(ABC):

    @abstractmethod
    def add(self, artist_name: str) -> None: ...

    @abstractmethod
    def get_all(self) -> set: ...

    @abstractmethod
    def close(self) -> None: ...
