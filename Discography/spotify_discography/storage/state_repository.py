import json
import sqlite3
import time
from typing import Optional

from .repository import StateRepository


class SQLiteStateRepository(StateRepository):

    def __init__(self, db_path: str):
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._create_tables()

    def _create_tables(self):
        self._conn.executescript("""
            CREATE TABLE IF NOT EXISTS artists (
                artist_id   TEXT PRIMARY KEY,
                artist_name TEXT NOT NULL,
                playlist_id TEXT,
                last_scan   REAL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS albums (
                album_id     TEXT NOT NULL,
                artist_id    TEXT NOT NULL REFERENCES artists(artist_id),
                album_name   TEXT NOT NULL,
                total_tracks INTEGER NOT NULL DEFAULT 0,
                last_checked REAL DEFAULT 0,
                PRIMARY KEY (album_id, artist_id)
            );

            CREATE TABLE IF NOT EXISTS playlist_tracks (
                artist_id TEXT NOT NULL REFERENCES artists(artist_id),
                track_id  TEXT NOT NULL,
                PRIMARY KEY (artist_id, track_id)
            );

            -- Reprise sur interruption (niveau album)
            -- album_ids      : JSON array des album_id dans l'ordre du scan
            -- last_album_idx : index du dernier album complètement traité
            --                  -1 = aucun album terminé, reprendre à 0
            CREATE TABLE IF NOT EXISTS album_checkpoint (
                artist_id      TEXT PRIMARY KEY,
                album_ids      TEXT    NOT NULL,
                last_album_idx INTEGER NOT NULL DEFAULT -1
            );

            -- Tracks indisponibles sur le marché de l'utilisateur
            CREATE TABLE IF NOT EXISTS unavailable_tracks (
                track_id    TEXT NOT NULL,
                artist_id   TEXT NOT NULL REFERENCES artists(artist_id),
                detected_at REAL NOT NULL,
                PRIMARY KEY (track_id, artist_id)
            );

            -- Tracks retirées manuellement de la playlist
            CREATE TABLE IF NOT EXISTS removed_tracks (
                track_id      TEXT NOT NULL,
                artist_id     TEXT NOT NULL REFERENCES artists(artist_id),
                detected_at   REAL NOT NULL,
                blacklisted   INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (track_id, artist_id)
            );

            -- État persistant du daemon
            -- last_followed_refresh : timestamp du dernier rechargement
            --   des artistes suivis (mis à jour uniquement quand tous les
            --   artistes ont été traités, i.e. aucun last_scan == 0)
            -- cycle_started_at / cycle_artist_idx : position dans le cycle
            CREATE TABLE IF NOT EXISTS daemon_state (
                id                    INTEGER PRIMARY KEY CHECK (id = 1),
                last_followed_refresh REAL    NOT NULL DEFAULT 0,
                cycle_started_at      REAL    NOT NULL DEFAULT 0,
                cycle_artist_idx      INTEGER NOT NULL DEFAULT 0
            );

            -- Historique des intervalles entre rate limits Spotify
            CREATE TABLE IF NOT EXISTS rate_limit_intervals (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                start_ts    REAL    NOT NULL,
                end_ts      REAL    NOT NULL,
                ok_calls    INTEGER NOT NULL DEFAULT 0,
                fail_calls  INTEGER NOT NULL DEFAULT 0,
                retry_after REAL    NOT NULL DEFAULT 0,
                endpoint    TEXT    NOT NULL DEFAULT ''
            );

            CREATE INDEX IF NOT EXISTS idx_rli_end_ts
                ON rate_limit_intervals(end_ts DESC);
            CREATE INDEX IF NOT EXISTS idx_artists_last_scan
                ON artists(last_scan ASC);
            CREATE INDEX IF NOT EXISTS idx_unavailable_artist
                ON unavailable_tracks(artist_id);
            CREATE INDEX IF NOT EXISTS idx_removed_artist
                ON removed_tracks(artist_id);
        """)
        self._conn.commit()

    # ── Artists ───────────────────────────────────────────────────────────────

    def get_artist(self, artist_id: str) -> Optional[dict]:
        row = self._conn.execute(
            "SELECT * FROM artists WHERE artist_id = ?", (artist_id,)
        ).fetchone()
        return dict(row) if row else None

    def upsert_artist(self, artist_id: str, data: dict) -> None:
        self._conn.execute("""
            INSERT INTO artists (artist_id, artist_name, playlist_id, last_scan)
            VALUES (:artist_id, :artist_name, :playlist_id, :last_scan)
            ON CONFLICT(artist_id) DO UPDATE SET
                artist_name = excluded.artist_name,
                playlist_id = excluded.playlist_id,
                last_scan   = excluded.last_scan
        """, {
            "artist_id":   artist_id,
            "artist_name": data["artist_name"],
            "playlist_id": data.get("playlist_id"),
            "last_scan":   data.get("last_scan", 0.0),
        })
        self._conn.commit()

    def get_all_artists(self) -> list:
        rows = self._conn.execute(
            "SELECT * FROM artists ORDER BY last_scan ASC"
        ).fetchall()
        return [dict(r) for r in rows]

    def get_ordered_artist_ids(self, followed_ids: set) -> list:
        """
        Retourne la liste ordonnée des artist_id à traiter dans ce cycle :

          1. Artistes avec un album_checkpoint actif (reprise prioritaire)
             → dans l'ordre de last_scan ASC pour être déterministe
          2. Artistes suivis jamais traités (last_scan == 0, pas de checkpoint)
          3. Artistes suivis déjà traités, triés par last_scan ASC
             (le plus ancien d'abord)

        Seuls les artistes présents dans followed_ids sont inclus,
        plus les artistes en cours de checkpoint même s'ils ont été
        désabonnés (pour terminer proprement leur scan en cours).
        """
        # Artistes avec checkpoint actif
        checkpoint_ids = {
            r[0] for r in self._conn.execute(
                "SELECT artist_id FROM album_checkpoint"
            ).fetchall()
        }

        # Tous les artistes connus en DB avec leur last_scan
        known = {
            r["artist_id"]: r["last_scan"]
            for r in self._conn.execute(
                "SELECT artist_id, last_scan FROM artists"
            ).fetchall()
        }

        # Groupe 1 : checkpoint actif (qu'ils soient encore suivis ou non)
        group1 = sorted(
            checkpoint_ids,
            key=lambda aid: known.get(aid, 0.0),
        )

        # Groupe 2 : suivis, jamais traités, sans checkpoint
        group2 = [
            aid for aid in followed_ids
            if aid not in checkpoint_ids
            and known.get(aid, 0.0) == 0.0
        ]

        # Groupe 3 : suivis, déjà traités, sans checkpoint, triés par last_scan ASC
        group3 = sorted(
            [
                aid for aid in followed_ids
                if aid not in checkpoint_ids
                and known.get(aid, 0.0) > 0.0
            ],
            key=lambda aid: known.get(aid, 0.0),
        )

        return group1 + group2 + group3

    def all_followed_scanned(self, followed_ids: set) -> bool:
        """
        Retourne True si tous les artistes suivis ont été traités au moins
        une fois (last_scan > 0) ET qu'aucun album_checkpoint n'est actif.

        C'est la condition pour déclencher un refresh de la liste des suivis.
        """
        if not followed_ids:
            return False

        # S'il reste des checkpoints actifs, le cycle n'est pas terminé
        pending = self._conn.execute(
            "SELECT COUNT(*) FROM album_checkpoint"
        ).fetchone()[0]
        if pending > 0:
            return False

        # S'il reste des artistes suivis avec last_scan == 0
        placeholders = ",".join("?" * len(followed_ids))
        unscanned = self._conn.execute(
            f"SELECT COUNT(*) FROM artists "
            f"WHERE artist_id IN ({placeholders}) AND last_scan = 0",
            list(followed_ids),
        ).fetchone()[0]

        return unscanned == 0

    # ── Albums ────────────────────────────────────────────────────────────────

    def get_artist_albums(self, artist_id: str) -> dict:
        rows = self._conn.execute(
            "SELECT * FROM albums WHERE artist_id = ?", (artist_id,)
        ).fetchall()
        return {r["album_id"]: dict(r) for r in rows}

    def upsert_album(self, artist_id: str, album_id: str, data: dict) -> None:
        self._conn.execute("""
            INSERT INTO albums (album_id, artist_id, album_name, total_tracks, last_checked)
            VALUES (:album_id, :artist_id, :album_name, :total_tracks, :last_checked)
            ON CONFLICT(album_id, artist_id) DO UPDATE SET
                album_name   = excluded.album_name,
                total_tracks = excluded.total_tracks,
                last_checked = excluded.last_checked
        """, {
            "album_id":     album_id,
            "artist_id":    artist_id,
            "album_name":   data["album_name"],
            "total_tracks": data["total_tracks"],
            "last_checked": data.get("last_checked", time.time()),
        })
        self._conn.commit()

    # ── Album checkpoint ──────────────────────────────────────────────────────

    def save_album_checkpoint(
        self,
        artist_id: str,
        album_ids: list,
        last_album_idx: int,
    ) -> None:
        """
        Crée ou met à jour le checkpoint album pour un artiste.
        album_ids  : liste ordonnée des album_id telle que récupérée depuis Spotify.
        last_album_idx : index du dernier album complètement traité (-1 si aucun).
        """
        self._conn.execute("""
            INSERT INTO album_checkpoint (artist_id, album_ids, last_album_idx)
            VALUES (?, ?, ?)
            ON CONFLICT(artist_id) DO UPDATE SET
                album_ids      = excluded.album_ids,
                last_album_idx = excluded.last_album_idx
        """, (artist_id, json.dumps(album_ids), last_album_idx))
        self._conn.commit()

    def load_album_checkpoint(self, artist_id: str) -> Optional[dict]:
        """
        Retourne le checkpoint album pour un artiste, ou None s'il n'existe pas.
        Retourne : { "album_ids": [...], "last_album_idx": int }
        """
        row = self._conn.execute(
            "SELECT album_ids, last_album_idx FROM album_checkpoint WHERE artist_id = ?",
            (artist_id,),
        ).fetchone()
        if not row:
            return None
        return {
            "album_ids":      json.loads(row["album_ids"]),
            "last_album_idx": row["last_album_idx"],
        }

    def clear_album_checkpoint(self, artist_id: str) -> None:
        """Supprime le checkpoint album une fois l'artiste entièrement traité."""
        self._conn.execute(
            "DELETE FROM album_checkpoint WHERE artist_id = ?", (artist_id,)
        )
        self._conn.commit()

    # ── Playlist tracks ───────────────────────────────────────────────────────

    def get_playlist_tracks(self, artist_id: str) -> set:
        rows = self._conn.execute(
            "SELECT track_id FROM playlist_tracks WHERE artist_id = ?", (artist_id,)
        ).fetchall()
        return {r["track_id"] for r in rows}

    def set_playlist_tracks(self, artist_id: str, track_ids: set) -> None:
        with self._conn:
            self._conn.execute(
                "DELETE FROM playlist_tracks WHERE artist_id = ?", (artist_id,)
            )
            self._conn.executemany(
                "INSERT OR IGNORE INTO playlist_tracks VALUES (?, ?)",
                [(artist_id, tid) for tid in track_ids],
            )

    def add_playlist_tracks(self, artist_id: str, track_ids: set) -> None:
        with self._conn:
            self._conn.executemany(
                "INSERT OR IGNORE INTO playlist_tracks VALUES (?, ?)",
                [(artist_id, tid) for tid in track_ids],
            )

    # ── Daemon state ──────────────────────────────────────────────────────────

    def load_daemon_state(self) -> dict:
        row = self._conn.execute(
            "SELECT * FROM daemon_state WHERE id = 1"
        ).fetchone()
        if row:
            return dict(row)
        return {
            "last_followed_refresh": 0.0,
            "cycle_started_at":      0.0,
            "cycle_artist_idx":      0,
        }

    def save_daemon_state(
        self,
        last_followed_refresh: float,
        cycle_started_at: float,
        cycle_artist_idx: int,
    ) -> None:
        self._conn.execute("""
            INSERT INTO daemon_state
                (id, last_followed_refresh, cycle_started_at, cycle_artist_idx)
            VALUES (1, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                last_followed_refresh = excluded.last_followed_refresh,
                cycle_started_at      = excluded.cycle_started_at,
                cycle_artist_idx      = excluded.cycle_artist_idx
        """, (last_followed_refresh, cycle_started_at, cycle_artist_idx))
        self._conn.commit()

    def update_daemon_artist_idx(self, cycle_artist_idx: int) -> None:
        self._conn.execute("""
            UPDATE daemon_state SET cycle_artist_idx = ? WHERE id = 1
        """, (cycle_artist_idx,))
        self._conn.commit()

    # ── Unavailable tracks ────────────────────────────────────────────────────

    def add_unavailable_tracks(self, artist_id: str, track_ids: set) -> None:
        with self._conn:
            self._conn.executemany("""
                INSERT OR IGNORE INTO unavailable_tracks (track_id, artist_id, detected_at)
                VALUES (?, ?, ?)
            """, [(tid, artist_id, time.time()) for tid in track_ids])

    def get_unavailable_tracks(self, artist_id: str) -> set:
        rows = self._conn.execute(
            "SELECT track_id FROM unavailable_tracks WHERE artist_id = ?", (artist_id,)
        ).fetchall()
        return {r["track_id"] for r in rows}

    def get_all_unavailable_tracks(self) -> list:
        rows = self._conn.execute("""
            SELECT ut.track_id, ut.artist_id, ar.artist_name, ut.detected_at
            FROM unavailable_tracks ut
            JOIN artists ar ON ar.artist_id = ut.artist_id
            ORDER BY ut.detected_at DESC
            LIMIT 1000
        """).fetchall()
        return [dict(r) for r in rows]

    # ── Removed tracks ────────────────────────────────────────────────────────

    def add_removed_tracks(self, artist_id: str, track_ids: set) -> None:
        with self._conn:
            self._conn.executemany("""
                INSERT OR IGNORE INTO removed_tracks
                    (track_id, artist_id, detected_at, blacklisted)
                VALUES (?, ?, ?, 0)
            """, [(tid, artist_id, time.time()) for tid in track_ids])

    def get_removed_tracks(self, artist_id: str) -> dict:
        rows = self._conn.execute("""
            SELECT track_id, detected_at, blacklisted
            FROM removed_tracks WHERE artist_id = ?
        """, (artist_id,)).fetchall()
        return {r["track_id"]: dict(r) for r in rows}

    def get_non_blacklisted_removed_tracks(self, artist_id: str) -> set:
        rows = self._conn.execute("""
            SELECT track_id FROM removed_tracks
            WHERE artist_id = ? AND blacklisted = 0
        """, (artist_id,)).fetchall()
        return {r["track_id"] for r in rows}

    def blacklist_track(self, artist_id: str, track_id: str) -> None:
        self._conn.execute("""
            UPDATE removed_tracks SET blacklisted = 1
            WHERE artist_id = ? AND track_id = ?
        """, (artist_id, track_id))
        self._conn.commit()

    def get_all_removed_tracks_blacklisted(self, artist_id: str) -> set:
        rows = self._conn.execute("""
            SELECT track_id FROM removed_tracks
            WHERE artist_id = ? AND blacklisted = 1
        """, (artist_id,)).fetchall()
        return {r["track_id"] for r in rows}

    def get_all_removed_tracks(self) -> list:
        rows = self._conn.execute("""
            SELECT rt.track_id, rt.artist_id, ar.artist_name,
                   rt.detected_at, rt.blacklisted
            FROM removed_tracks rt
            JOIN artists ar ON ar.artist_id = rt.artist_id
            ORDER BY rt.detected_at DESC
            LIMIT 1000
        """).fetchall()
        return [dict(r) for r in rows]

    # ── Rate limit intervals ──────────────────────────────────────────────────

    def save_rate_limit_interval(
        self,
        start_ts: float,
        end_ts: float,
        ok_calls: int,
        fail_calls: int,
        retry_after: float,
        endpoint: str,
    ) -> None:
        self._conn.execute("""
            INSERT INTO rate_limit_intervals
                (start_ts, end_ts, ok_calls, fail_calls, retry_after, endpoint)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (start_ts, end_ts, ok_calls, fail_calls, retry_after, endpoint))
        self._conn.commit()

    def get_rate_limit_intervals(self, limit: int = 100) -> list:
        rows = self._conn.execute("""
            SELECT id, start_ts, end_ts, ok_calls, fail_calls,
                   retry_after, endpoint
            FROM rate_limit_intervals
            ORDER BY end_ts DESC
            LIMIT ?
        """, (limit,)).fetchall()
        result = []
        for r in rows:
            d = dict(r)
            d["total_calls"] = d["ok_calls"] + d["fail_calls"]
            result.append(d)
        return result

    def get_rate_limit_stats(self) -> dict:
        row = self._conn.execute("""
            SELECT
                COUNT(*)                   AS total_intervals,
                AVG(ok_calls)              AS avg_ok,
                AVG(fail_calls)            AS avg_fail,
                AVG(ok_calls + fail_calls) AS avg_total,
                AVG(retry_after)           AS avg_retry_after,
                MAX(retry_after)           AS max_retry_after
            FROM rate_limit_intervals
        """).fetchone()
        if not row or not row["total_intervals"]:
            return {"total_intervals": 0}
        return {
            "total_intervals": row["total_intervals"],
            "avg_ok_calls":    round(row["avg_ok"]    or 0, 1),
            "avg_fail_calls":  round(row["avg_fail"]  or 0, 1),
            "avg_total_calls": round(row["avg_total"] or 0, 1),
            "avg_retry_after": round(row["avg_retry_after"] or 0, 1),
            "max_retry_after": row["max_retry_after"] or 0,
        }

    # ── Close ─────────────────────────────────────────────────────────────────

    def close(self) -> None:
        self._conn.close()
