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

            -- [1] Reprise sur interruption et 429
            CREATE TABLE IF NOT EXISTS run_checkpoint (
                id              INTEGER PRIMARY KEY CHECK (id = 1),
                last_artist_idx INTEGER NOT NULL DEFAULT 0,
                run_started_at  REAL    NOT NULL DEFAULT 0
            );

            -- [2-A] Tracks indisponibles
            CREATE TABLE IF NOT EXISTS unavailable_tracks (
                track_id    TEXT NOT NULL,
                artist_id   TEXT NOT NULL REFERENCES artists(artist_id),
                detected_at REAL NOT NULL,
                PRIMARY KEY (track_id, artist_id)
            );

            -- [2-B] Tracks retirées manuellement
            CREATE TABLE IF NOT EXISTS removed_tracks (
                track_id      TEXT NOT NULL,
                artist_id     TEXT NOT NULL REFERENCES artists(artist_id),
                detected_at   REAL NOT NULL,
                blacklisted   INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (track_id, artist_id)
            );

            -- [DAEMON] État persistant du cycle daemon
            -- Mémorise la dernière fois que la liste des artistes suivis a été rechargée
            -- et le timestamp de début du cycle courant.
            CREATE TABLE IF NOT EXISTS daemon_state (
                id                      INTEGER PRIMARY KEY CHECK (id = 1),
                last_followed_refresh   REAL NOT NULL DEFAULT 0,
                cycle_started_at        REAL NOT NULL DEFAULT 0,
                cycle_artist_idx        INTEGER NOT NULL DEFAULT 0
            );

            -- [429] Historique des intervalles entre rate limits.
            -- Chaque ligne correspond à un intervalle clos par un 429 Spotify.
            -- ok_calls   = nombre d'appels ayant abouti dans cet intervalle
            -- fail_calls = nombre d'appels ayant échoué (429 lui-même + 5xx + réseau)
            -- total est calculé à l'affichage : ok_calls + fail_calls
            CREATE TABLE IF NOT EXISTS rate_limit_intervals (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                start_ts     REAL    NOT NULL,
                end_ts       REAL    NOT NULL,
                ok_calls     INTEGER NOT NULL DEFAULT 0,
                fail_calls   INTEGER NOT NULL DEFAULT 0,
                retry_after  REAL    NOT NULL DEFAULT 0,
                endpoint     TEXT    NOT NULL DEFAULT ''
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

    def get_artists_ordered_by_scan(self) -> list:
        """
        Retourne tous les artistes triés par last_scan ASC.
        Utilisé par le daemon pour trouver le prochain artiste à scanner
        (celui dont le scan est le plus ancien).
        """
        rows = self._conn.execute(
            "SELECT * FROM artists ORDER BY last_scan ASC"
        ).fetchall()
        return [dict(r) for r in rows]

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
                [(artist_id, tid) for tid in track_ids]
            )

    def add_playlist_tracks(self, artist_id: str, track_ids: set) -> None:
        with self._conn:
            self._conn.executemany(
                "INSERT OR IGNORE INTO playlist_tracks VALUES (?, ?)",
                [(artist_id, tid) for tid in track_ids]
            )

    # ── [1] Run checkpoint ────────────────────────────────────────────────────

    def save_checkpoint(self, last_artist_idx: int, run_started_at: float) -> None:
        self._conn.execute("""
            INSERT INTO run_checkpoint (id, last_artist_idx, run_started_at)
            VALUES (1, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                last_artist_idx = excluded.last_artist_idx,
                run_started_at  = excluded.run_started_at
        """, (last_artist_idx, run_started_at))
        self._conn.commit()

    def load_checkpoint(self) -> Optional[dict]:
        row = self._conn.execute(
            "SELECT * FROM run_checkpoint WHERE id = 1"
        ).fetchone()
        return dict(row) if row else None

    def clear_checkpoint(self) -> None:
        self._conn.execute("DELETE FROM run_checkpoint WHERE id = 1")
        self._conn.commit()

    # ── [DAEMON] État du cycle daemon ─────────────────────────────────────────

    def load_daemon_state(self) -> dict:
        """
        Charge l'état persistant du daemon.
        Retourne un dict avec :
          - last_followed_refresh : timestamp du dernier rechargement des artistes suivis
          - cycle_started_at      : timestamp de début du cycle courant
          - cycle_artist_idx      : index de l'artiste en cours dans le cycle
        """
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
        """Met à jour uniquement l'index de l'artiste courant (appel fréquent)."""
        self._conn.execute("""
            UPDATE daemon_state SET cycle_artist_idx = ? WHERE id = 1
        """, (cycle_artist_idx,))
        self._conn.commit()

    # ── [2-A] Tracks indisponibles ────────────────────────────────────────────

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

    # ── [2-B] Tracks retirées ─────────────────────────────────────────────────

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

    # ── [429] Rate limit intervals ─────────────────────────────────────────────

    def save_rate_limit_interval(
        self,
        start_ts: float,
        end_ts: float,
        ok_calls: int,
        fail_calls: int,
        retry_after: float,
        endpoint: str,
    ) -> None:
        """
        Persiste un intervalle clos (déclenché par un 429).
        Calcul du total à l'affichage : ok_calls + fail_calls.
        """
        self._conn.execute("""
            INSERT INTO rate_limit_intervals
                (start_ts, end_ts, ok_calls, fail_calls, retry_after, endpoint)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (start_ts, end_ts, ok_calls, fail_calls, retry_after, endpoint))
        self._conn.commit()

    def get_rate_limit_intervals(self, limit: int = 100) -> list:
        """
        Retourne les derniers intervalles triés du plus récent au plus ancien.
        Chaque dict contient :
          start_ts, end_ts, ok_calls, fail_calls, retry_after, endpoint
        Le champ total_calls (ok + fail) est ajouté ici pour commodité.
        """
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
        """
        Agrégats globaux sur tous les intervalles persistés :
          - total_intervals   : nombre de 429 enregistrés
          - avg_ok_calls      : moyenne des appels réussis par intervalle
          - avg_fail_calls    : moyenne des appels en erreur par intervalle
          - avg_total_calls   : moyenne du total par intervalle
          - avg_retry_after   : moyenne des Retry-After (secondes)
          - max_retry_after   : Retry-After le plus long observé
        """
        row = self._conn.execute("""
            SELECT
                COUNT(*)              AS total_intervals,
                AVG(ok_calls)         AS avg_ok,
                AVG(fail_calls)       AS avg_fail,
                AVG(ok_calls + fail_calls) AS avg_total,
                AVG(retry_after)      AS avg_retry_after,
                MAX(retry_after)      AS max_retry_after
            FROM rate_limit_intervals
        """).fetchone()
        if not row:
            return {}
        return {
            "total_intervals": row["total_intervals"],
            "avg_ok_calls":    round(row["avg_ok"]   or 0, 1),
            "avg_fail_calls":  round(row["avg_fail"] or 0, 1),
            "avg_total_calls": round(row["avg_total"] or 0, 1),
            "avg_retry_after": round(row["avg_retry_after"] or 0, 1),
            "max_retry_after": row["max_retry_after"] or 0,
        }

    # ── Close ─────────────────────────────────────────────────────────────────

    def close(self) -> None:
        self._conn.close()
