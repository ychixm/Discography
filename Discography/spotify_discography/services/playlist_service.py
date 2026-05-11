"""
playlist_service.py
===================
Gestion des playlists Spotify avec support multi-slots.

Quand une playlist atteint SPOTIFY_PLAYLIST_MAX_TRACKS (10 000 tracks),
un nouveau slot est créé automatiquement :
  - Slot 1 → "Artiste - Discography"
  - Slot 2 → "Artiste 2 - Discography"
  - Slot N → "Artiste N - Discography"

La méthode get_playlist_tracks() dans le repo retourne l'union de TOUS les
slots (table playlist_tracks), ce qui garantit qu'une track n'est jamais
ajoutée deux fois quelle que soit la playlist dans laquelle elle se trouve.

load_existing_playlists() reconnaît les deux patterns de nommage et
synchronise les slots en DB, y compris la mise à jour des track_count
depuis Spotify (appel /playlists/{id}?fields=tracks(total)).
"""

import logging
import re
from typing import Optional

from ..api.client import SpotifyClient
from ..storage.repository import StateRepository
from .. import config

logger = logging.getLogger("spotify_discography")

SPOTIFY_PLAYLIST_MAX_TRACKS = 10_000


def playlist_name_for_slot(artist_name: str, slot: int) -> str:
    """
    Slot 1 → "Artiste - Discography"
    Slot N → "Artiste N - Discography"  (N ≥ 2)
    """
    if slot == 1:
        return f"{artist_name} - Discography"
    return f"{artist_name} {slot} - Discography"


def _slot_from_playlist_name(artist_name: str, name: str) -> Optional[int]:
    """
    Déduit le slot depuis le nom d'une playlist.
    Retourne None si le nom ne correspond pas au pattern de cet artiste.
    """
    # Pattern slot 1 : "Artiste - Discography"
    if name == f"{artist_name} - Discography":
        return 1
    # Pattern slot N : "Artiste N - Discography"
    pattern = re.compile(
        r"^" + re.escape(artist_name) + r" (\d+) - Discography$"
    )
    m = pattern.match(name)
    if m:
        return int(m.group(1))
    return None



class PlaylistService:

    def __init__(self, client: SpotifyClient, repo: StateRepository, me_id: str):
        self._client = client
        self._repo   = repo
        self._me_id  = me_id
        # Cache RAM : playlist_name → playlist_id (tous slots, tous artistes)
        self._name_to_id: dict = {}

    # ── Chargement initial ────────────────────────────────────────────────────

    def load_existing_playlists(self):
        """
        Charge toutes les playlists de l'utilisateur depuis Spotify et
        synchronise la table artist_playlists en DB.

        Pour chaque artiste connu :
          1. Cherche toutes ses playlists (slot 1, 2, …) dans les playlists Spotify.
          2. Pour chaque slot trouvé, vérifie / crée l'entrée en DB et
             met à jour le track_count depuis Spotify (GET /playlists/{id}).
        """
        # Charge toutes les playlists Spotify de l'utilisateur
        raw_playlists = self._client.paginate(
            f"{config.API_BASE}/me/playlists",
            params={"limit": config.LIMIT_PLAYLISTS},
        )
        # Filtre : seulement les playlists appartenant à cet utilisateur
        my_playlists = [
            p for p in raw_playlists
            if p and p["owner"]["id"] == self._me_id
        ]
        # Reconstruit le cache RAM nom → id
        self._name_to_id = {p["name"]: p["id"] for p in my_playlists}

        # Synchronise chaque artiste connu
        for artist in self._repo.get_all_artists():
            artist_id   = artist["artist_id"]
            artist_name = artist["artist_name"]
            self._sync_artist_slots(artist_id, artist_name)

        logger.info(
            "Playlists chargées en mémoire : %d (utilisateur %s)",
            len(self._name_to_id), self._me_id,
        )

    def _sync_artist_slots(self, artist_id: str, artist_name: str):
        """
        Pour un artiste donné, détecte tous ses slots Spotify existants,
        crée / met à jour les entrées en DB et peuple artists.playlist_id
        (slot 1) pour la rétrocompatibilité du dashboard.

        Chaque track_count est récupéré depuis Spotify puis immédiatement
        sauvegardé en DB pour que la valeur soit persistée même si
        load_existing_playlists() est interrompue à mi-chemin.
        """
        slot = 1
        while True:
            name = playlist_name_for_slot(artist_name, slot)
            pid  = self._name_to_id.get(name)
            if pid is None:
                # Plus de slot à cet index → on s'arrête
                break

            # Récupère le track_count réel depuis Spotify
            track_count = self._fetch_playlist_track_count(pid)

            # Persiste le slot en DB immédiatement après le fetch
            self._repo.upsert_artist_playlist(artist_id, slot, pid, track_count)
            logger.info(
                "Sync slot %d '%s' (%s) : track_count=%d sauvegardé en DB",
                slot, artist_name, pid, track_count,
            )

            # Rétrocompatibilité : garder artists.playlist_id = slot 1
            if slot == 1:
                db_artist = self._repo.get_artist(artist_id)
                if db_artist and db_artist.get("playlist_id") != pid:
                    self._repo.upsert_artist(artist_id, {
                        "artist_name": artist_name,
                        "playlist_id": pid,
                        "last_scan":   db_artist.get("last_scan", 0.0),
                    })
                    logger.info(
                        "Sync DB artists.playlist_id '%s' (%s) → %s",
                        name, artist_id, pid,
                    )

            slot += 1

    def _fetch_playlist_track_count(self, playlist_id: str) -> int:
        """
        Interroge Spotify pour connaître le nombre de tracks dans une playlist.
        Retourne 0 en cas d'erreur (défensif).

        Note : le paramètre fields=tracks.total s'avère non fiable sur certaines
        playlists (champ 'tracks' absent de la réponse). On interroge directement
        l'endpoint complet pour garantir la cohérence, au prix d'une réponse
        légèrement plus volumineuse.
        """
        try:
            logger.debug(
                "FETCH track_count playlist %s", playlist_id,
            )
            r    = self._client.get(f"{config.API_BASE}/playlists/{playlist_id}")
            data = r.json()

            tracks = data.get("tracks")
            if isinstance(tracks, dict):
                count = int(tracks.get("total", 0))
                logger.debug(
                    "FETCH track_count playlist %s → %d tracks",
                    playlist_id, count,
                )
                return count

            # Réponse structurellement inattendue — on logue et retourne 0
            logger.warning(
                "Réponse inattendue pour GET /playlists/%s "
                "(champ 'tracks' : %r) — track_count=0 utilisé par défaut",
                playlist_id, tracks,
            )
            return 0

        except Exception as e:
            logger.warning(
                "Impossible de lire le track_count de la playlist %s : %s — "
                "valeur 0 utilisée par défaut",
                playlist_id, e,
            )
            return 0

    # ── Obtention / création de la playlist courante ──────────────────────────

    def get_or_create_playlist(self, artist_id: str, artist_name: str) -> str:
        """
        Retourne le playlist_id du slot courant non plein pour cet artiste.

        Logique :
          1. Charge les slots existants depuis la DB (triés par slot ASC).
          2. Cherche le dernier slot dont track_count < SPOTIFY_PLAYLIST_MAX_TRACKS.
          3. Si aucun slot n'existe ou si tous sont pleins → crée un nouveau slot.

        Retourne toujours un playlist_id valide.
        """
        slots = self._repo.get_artist_playlists(artist_id)

        if slots:
            # Le dernier slot est le candidat naturel pour les ajouts
            last = slots[-1]
            if last["track_count"] < SPOTIFY_PLAYLIST_MAX_TRACKS:
                logger.debug(
                    "Slot %d pour '%s' : %s (%d/%d tracks)",
                    last["slot"], artist_name,
                    last["playlist_id"], last["track_count"],
                    SPOTIFY_PLAYLIST_MAX_TRACKS,
                )
                return last["playlist_id"]

            # Tous les slots existants sont pleins → nouveau slot
            next_slot = last["slot"] + 1
        else:
            next_slot = 1

        return self._create_slot(artist_id, artist_name, next_slot)

    def _create_slot(self, artist_id: str, artist_name: str, slot: int) -> str:
        """
        Crée une nouvelle playlist Spotify pour ce slot et enregistre
        le slot en DB.
        """
        name = playlist_name_for_slot(artist_name, slot)
        pid  = self._create_playlist(name)

        self._repo.upsert_artist_playlist(artist_id, slot, pid, track_count=0)
        self._name_to_id[name] = pid

        # Rétrocompatibilité : met à jour artists.playlist_id pour slot 1
        if slot == 1:
            db_artist = self._repo.get_artist(artist_id)
            self._repo.upsert_artist(artist_id, {
                "artist_name": artist_name,
                "playlist_id": pid,
                "last_scan":   db_artist.get("last_scan", 0.0) if db_artist else 0.0,
            })

        logger.info(
            "Nouveau slot %d pour '%s' : playlist '%s' (%s)",
            slot, artist_name, name, pid,
        )
        return pid

    def _create_playlist(self, name: str) -> str:
        r   = self._client.post(
            f"{config.API_BASE}/me/playlists",
            json={"name": name, "public": True},
        )
        pid = r.json()["id"]
        logger.info("Nouvelle playlist créée : '%s' (%s)", name, pid)
        return pid

    # ── Ajout de tracks (avec dispatch multi-slots) ───────────────────────────

    def add_tracks(self, artist_id: str, artist_name: str, track_ids: set) -> int:
        """
        Ajoute les nouvelles tracks à la (ou aux) playlist(s) de l'artiste.

        Flux :
          1. Calcule les tracks vraiment nouvelles (non présentes dans le cache
             global artist_id → toutes playlists confondues).
          2. Dispatche par blocs de 100 vers le slot courant.
             Si un slot est plein avant la fin, crée le slot suivant et continue.
          3. Met à jour le cache DB (playlist_tracks + artist_playlists.track_count).

        Retourne le nombre total de tracks effectivement ajoutées.
        """
        cached     = self._repo.get_playlist_tracks(artist_id)
        new_tracks = list(track_ids - cached)

        if not new_tracks:
            logger.info("Aucune nouvelle track pour '%s'", artist_name)
            return 0

        total_added = 0
        remaining   = list(new_tracks)

        while remaining:
            playlist_id = self.get_or_create_playlist(artist_id, artist_name)

            # Capacité restante dans le slot courant
            slots        = self._repo.get_artist_playlists(artist_id)
            current_slot = next(
                (s for s in reversed(slots) if s["playlist_id"] == playlist_id),
                None,
            )
            if current_slot is None:
                # Slot tout juste créé — track_count = 0
                available = SPOTIFY_PLAYLIST_MAX_TRACKS
                slot_num  = slots[-1]["slot"] if slots else 1
            else:
                available = SPOTIFY_PLAYLIST_MAX_TRACKS - current_slot["track_count"]
                slot_num  = current_slot["slot"]

            if available <= 0:
                # Normalement get_or_create_playlist aurait dû créer un nouveau
                # slot — situation défensive : on force la création
                logger.warning(
                    "Slot %d de '%s' plein, forçage du slot suivant",
                    slot_num, artist_name,
                )
                slot_num   = slot_num + 1
                playlist_id = self._create_slot(artist_id, artist_name, slot_num)
                available   = SPOTIFY_PLAYLIST_MAX_TRACKS

            # Prend le sous-ensemble qui tient dans ce slot
            batch_for_slot = remaining[:available]
            remaining      = remaining[available:]

            # Envoie par batches de 100 (limite API Spotify)
            added_in_slot = 0
            for i in range(0, len(batch_for_slot), 100):
                batch = batch_for_slot[i:i + 100]
                uris  = [f"spotify:track:{t}" for t in batch]
                self._client.post(
                    f"{config.API_BASE}/playlists/{playlist_id}/items",
                    json={"uris": uris},
                )
                added_in_slot += len(batch)
                logger.info(
                    "Slot %d — batch %d : %d tracks ajoutées",
                    slot_num, i // 100 + 1, len(batch),
                )

            # Mise à jour du compteur du slot en DB
            self._repo.increment_artist_playlist_track_count(
                artist_id, slot_num, added_in_slot
            )
            new_total = self._repo.get_artist_playlist_track_count(
                artist_id, slot_num
            )
            logger.info(
                "Slot %d '%s' (%s) : track_count mis à jour → %d/%d (delta +%d)",
                slot_num, artist_name, playlist_id,
                new_total, SPOTIFY_PLAYLIST_MAX_TRACKS, added_in_slot,
            )
            total_added += added_in_slot

        # Mise à jour du cache global des tracks (toutes playlists confondues)
        self._repo.add_playlist_tracks(artist_id, set(new_tracks[:total_added]))
        logger.info(
            "%d tracks ajoutées et cachées pour '%s'",
            total_added, artist_name,
        )
        return total_added

    # ── Resync forcé ──────────────────────────────────────────────────────────

    def force_resync_playlist_tracks(
        self,
        artist_id: str,
        artist_name: str,
    ) -> set:
        """
        Recharge depuis Spotify le contenu de TOUS les slots d'un artiste,
        reconstruit le cache playlist_tracks en DB et met à jour les
        track_count dans artist_playlists.

        Utilisé pour corriger une incohérence entre la DB et Spotify.
        """
        logger.info(
            "FORCE_RESYNC toutes les playlists pour '%s' (%s)",
            artist_name, artist_id,
        )
        slots      = self._repo.get_artist_playlists(artist_id)
        all_tracks: set = set()

        for slot in slots:
            pid = slot["playlist_id"]
            items = self._client.paginate(
                f"{config.API_BASE}/playlists/{pid}/items",
                params={"limit": config.LIMIT_PLAYLIST_ITEMS},
            )
            track_ids = {
                it["track"]["id"]
                for it in items
                if it.get("track") and it["track"] and it["track"].get("id")
            }
            all_tracks |= track_ids
            # Met à jour le track_count réel pour ce slot
            self._repo.upsert_artist_playlist(
                artist_id, slot["slot"], pid, len(track_ids)
            )
            logger.info(
                "Resync slot %d ('%s') : %d tracks",
                slot["slot"], pid, len(track_ids),
            )

        self._repo.set_playlist_tracks(artist_id, all_tracks)
        logger.info(
            "Resync terminé pour '%s' : %d tracks au total",
            artist_name, len(all_tracks),
        )
        return all_tracks
