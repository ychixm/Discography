"""
dashboard_server.py
===================
Serveur HTTP léger pour le dashboard.
v2 : gestion du statut "rate_limited", nouvelles routes
     /api/db/removed-tracks, /api/db/unavailable-tracks,
     /api/db/artist-detail, /api/blacklist-track,
     /api/setup/create-config.
"""
import json
import sqlite3
import threading
import os
import time
import logging
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

from . import config

logger = logging.getLogger("spotify_discography")

# ── État partagé ──────────────────────────────────────────────────────────────
_run_state: dict = {
    "status":         "idle",
    "current_artist": None,
    "current_idx":    0,
    "total_artists":  0,
    "run_start":      None,
    "stats": {
        "artists_processed": 0,
        "artists_skipped":   0,
        "tracks_added":      0,
        "api_total_calls":   0,
        "api_429":           0,
        "api_5xx":           0,
        "api_detail":        {},
        "endpoint_stats":    {},
    },
    "log_tail":       [],
    "call_history":   [],
}
_run_lock = threading.Lock()

LOG_TAIL_MAX  = 300
CALL_HIST_MAX = 60


def update_run_state(**kwargs):
    with _run_lock:
        for k, v in kwargs.items():
            if k == "stats" and isinstance(v, dict):
                _run_state["stats"].update(v)
            else:
                _run_state[k] = v


def push_log(level: str, message: str):
    with _run_lock:
        _run_state["log_tail"].append({
            "ts":      time.time(),
            "level":   level,
            "message": message,
        })
        if len(_run_state["log_tail"]) > LOG_TAIL_MAX:
            _run_state["log_tail"].pop(0)


def record_api_call():
    with _run_lock:
        now = time.time()
        _run_state["call_history"].append(now)
        _run_state["call_history"] = [
            t for t in _run_state["call_history"]
            if now - t <= CALL_HIST_MAX
        ]


# ── Helpers DB ────────────────────────────────────────────────────────────────
def _db_connect() -> sqlite3.Connection:
    uri = f"file:{config.STATE_DB_PATH}?mode=ro"
    conn = sqlite3.connect(uri, uri=True, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def _db_stats() -> dict:
    try:
        conn = _db_connect()
        artists  = conn.execute("SELECT COUNT(*) FROM artists").fetchone()[0]
        albums   = conn.execute("SELECT COUNT(*) FROM albums").fetchone()[0]
        tracks   = conn.execute("SELECT COUNT(*) FROM playlist_tracks").fetchone()[0]
        excluded = 0
        try:
            excluded = conn.execute(
                "SELECT COUNT(*) FROM excluded_artists"
            ).fetchone()[0]
        except Exception:
            pass
        conn.close()
        return {
            "artists":          artists,
            "albums":           albums,
            "playlist_tracks":  tracks,
            "excluded_artists": excluded,
            "db_path":          config.STATE_DB_PATH,
        }
    except Exception as e:
        return {"error": str(e)}


def _albums_list(limit: int = 200) -> list:
    try:
        conn = _db_connect()
        rows = conn.execute("""
            SELECT al.album_id, al.artist_id, ar.artist_name,
                   al.album_name, al.total_tracks, al.last_checked
            FROM albums al
            JOIN artists ar ON ar.artist_id = al.artist_id
            ORDER BY al.last_checked DESC
            LIMIT ?
        """, (limit,)).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception as e:
        return [{"error": str(e)}]


def _playlists_list() -> list:
    try:
        conn = _db_connect()
        rows = conn.execute("""
            SELECT ar.artist_id, ar.artist_name, ar.playlist_id,
                   COUNT(pt.track_id) AS track_count
            FROM artists ar
            LEFT JOIN playlist_tracks pt ON pt.artist_id = ar.artist_id
            WHERE ar.playlist_id IS NOT NULL
            GROUP BY ar.artist_id
            ORDER BY track_count DESC
            LIMIT 200
        """).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception as e:
        return [{"error": str(e)}]


def _excluded_list() -> list:
    try:
        conn = _db_connect()
        rows = conn.execute(
            "SELECT artist_name, first_seen FROM excluded_artists "
            "ORDER BY first_seen DESC LIMIT 500"
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception:
        return []


def _removed_tracks_list() -> list:
    try:
        conn = _db_connect()
        rows = conn.execute("""
            SELECT rt.track_id, rt.artist_id, ar.artist_name,
                   rt.detected_at, rt.blacklisted
            FROM removed_tracks rt
            JOIN artists ar ON ar.artist_id = rt.artist_id
            ORDER BY rt.detected_at DESC
            LIMIT 1000
        """).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception:
        return []


def _unavailable_tracks_list() -> list:
    try:
        conn = _db_connect()
        rows = conn.execute("""
            SELECT ut.track_id, ut.artist_id, ar.artist_name, ut.detected_at
            FROM unavailable_tracks ut
            JOIN artists ar ON ar.artist_id = ut.artist_id
            ORDER BY ut.detected_at DESC
            LIMIT 1000
        """).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception:
        return []


def _artist_detail(artist_id: str) -> dict:
    try:
        conn = _db_connect()
        artist = conn.execute(
            "SELECT * FROM artists WHERE artist_id = ?", (artist_id,)
        ).fetchone()
        if not artist:
            conn.close()
            return {"error": "Artiste introuvable"}
        artist = dict(artist)

        albums = conn.execute("""
            SELECT album_name, total_tracks, last_checked
            FROM albums WHERE artist_id = ?
            ORDER BY last_checked DESC
        """, (artist_id,)).fetchall()
        artist["albums"] = [dict(a) for a in albums]

        track_count = conn.execute(
            "SELECT COUNT(*) FROM playlist_tracks WHERE artist_id = ?",
            (artist_id,)
        ).fetchone()[0]
        artist["track_count"] = track_count

        conn.close()
        return artist
    except Exception as e:
        return {"error": str(e)}


def _blacklist_track_rw(artist_id: str, track_id: str) -> dict:
    """Écriture dans la DB (pas en lecture seule)."""
    try:
        conn = sqlite3.connect(config.STATE_DB_PATH, check_same_thread=False)
        conn.execute("""
            UPDATE removed_tracks SET blacklisted = 1
            WHERE artist_id = ? AND track_id = ?
        """, (artist_id, track_id))
        conn.commit()
        conn.close()
        return {"ok": True}
    except Exception as e:
        return {"error": str(e)}


def _create_config_file(data: dict) -> dict:
    """
    Crée config.json dans APP_DATA_DIR (même emplacement que la DB et les logs).
    Le chemin effectif est celui que config._CONFIG_PATH utilise.
    """
    # On reconstruit le chemin de la même façon que config.py,
    # sans dépendre d'un import de config (qui pourrait être périmé).
    import platform

    def _app_data_dir() -> str:
        system = platform.system()
        if system == "Windows":
            base = os.environ.get("APPDATA") or os.path.expanduser("~")
        elif system == "Darwin":
            base = os.path.join(os.path.expanduser("~"), "Library", "Application Support")
        else:
            base = os.environ.get("XDG_DATA_HOME") or os.path.join(os.path.expanduser("~"), ".local", "share")
        path = os.path.join(base, "SpotifyDiscography")
        os.makedirs(path, exist_ok=True)
        return path

    config_path = os.environ.get(
        "SPOTIFY_CONFIG_PATH",
        os.path.join(_app_data_dir(), "config.json"),
    )

    if os.path.exists(config_path):
        return {"error": f"config.json existe déjà ({config_path})"}
    try:
        payload = {
            "client_id":     data["client_id"],
            "client_secret": data["client_secret"],
            "redirect_uri":  data["redirect_uri"],
        }
        tmp = config_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
        os.replace(tmp, config_path)
        logger.info("config.json créé dans : %s", config_path)
        return {"ok": True, "path": config_path}
    except Exception as e:
        return {"error": str(e)}


def _rate_limit_intervals_list(limit: int = 100) -> list:
    try:
        conn = _db_connect()
        rows = conn.execute("""
            SELECT id, start_ts, end_ts, ok_calls, fail_calls,
                   retry_after, endpoint
            FROM rate_limit_intervals
            ORDER BY end_ts DESC
            LIMIT ?
        """, (limit,)).fetchall()
        conn.close()
        result = []
        for r in rows:
            d = dict(r)
            d["total_calls"] = d["ok_calls"] + d["fail_calls"]
            result.append(d)
        return result
    except Exception:
        return []


def _rate_limit_stats() -> dict:
    try:
        conn = _db_connect()
        row = conn.execute("""
            SELECT
                COUNT(*)                   AS total_intervals,
                AVG(ok_calls)              AS avg_ok,
                AVG(fail_calls)            AS avg_fail,
                AVG(ok_calls + fail_calls) AS avg_total,
                AVG(retry_after)           AS avg_retry_after,
                MAX(retry_after)           AS max_retry_after
            FROM rate_limit_intervals
        """).fetchone()
        conn.close()
        if not row or not row["total_intervals"]:
            return {"total_intervals": 0}
        return {
            "total_intervals": row["total_intervals"],
            "avg_ok_calls":    round(row["avg_ok"]    or 0, 1),
            "avg_fail_calls":  round(row["avg_fail"]   or 0, 1),
            "avg_total_calls": round(row["avg_total"]  or 0, 1),
            "avg_retry_after": round(row["avg_retry_after"] or 0, 1),
            "max_retry_after": row["max_retry_after"] or 0,
        }
    except Exception as e:
        return {"error": str(e)}


# ── Request handler ───────────────────────────────────────────────────────────
DASHBOARD_DIR = os.path.join(os.path.dirname(__file__), "dashboard")

MIME = {
    ".html": "text/html; charset=utf-8",
    ".css":  "text/css",
    ".js":   "application/javascript",
    ".ico":  "image/x-icon",
    ".png":  "image/png",
    ".svg":  "image/svg+xml",
}


class DashboardHandler(BaseHTTPRequestHandler):

    def log_message(self, fmt, *args):
        pass

    def _send_json(self, data, status: int = 200):
        body = json.dumps(data, ensure_ascii=False, default=str).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def _send_file(self, path: str):
        ext  = os.path.splitext(path)[1].lower()
        mime = MIME.get(ext, "application/octet-stream")
        try:
            with open(path, "rb") as f:
                data = f.read()
            self.send_response(200)
            self.send_header("Content-Type", mime)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        except FileNotFoundError:
            self.send_error(404)

    def _read_body(self) -> dict:
        length = int(self.headers.get("Content-Length", 0))
        if not length:
            return {}
        raw = self.rfile.read(length)
        try:
            return json.loads(raw)
        except Exception:
            return {}

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self):
        parsed = urlparse(self.path)
        path   = parsed.path.rstrip("/") or "/"
        qs     = parse_qs(parsed.query)

        if path in ("/", "/index.html"):
            self._send_file(os.path.join(DASHBOARD_DIR, "index.html"))
            return

        if path == "/setup":
            self._send_file(os.path.join(DASHBOARD_DIR, "setup.html"))
            return

        if path == "/api/run":
            with _run_lock:
                data = dict(_run_state)
                data["log_tail"]     = list(data["log_tail"])
                data["call_history"] = list(data["call_history"])
                data["stats"]        = dict(data["stats"])
                if "endpoint_stats" in data["stats"]:
                    data["stats"]["endpoint_stats"] = {
                        k: dict(v)
                        for k, v in data["stats"]["endpoint_stats"].items()
                    }
            self._send_json(data)

        elif path == "/api/stats/endpoints":
            with _run_lock:
                ep = {k: dict(v) for k, v in _run_state["stats"].get("endpoint_stats", {}).items()}
            self._send_json(ep)

        elif path == "/api/db/stats":
            self._send_json(_db_stats())

        elif path == "/api/db/albums":
            limit = int(qs.get("limit", [200])[0])
            self._send_json(_albums_list(limit))

        elif path == "/api/db/playlists":
            self._send_json(_playlists_list())

        elif path == "/api/db/excluded":
            self._send_json(_excluded_list())

        elif path == "/api/db/removed-tracks":
            self._send_json(_removed_tracks_list())

        elif path == "/api/db/unavailable-tracks":
            self._send_json(_unavailable_tracks_list())

        elif path == "/api/db/artist-detail":
            artist_id = qs.get("id", [None])[0]
            if not artist_id:
                self._send_json({"error": "id manquant"}, 400)
            else:
                self._send_json(_artist_detail(artist_id))

        elif path == "/api/db/rate-limit-intervals":
            limit = int(qs.get("limit", [100])[0])
            self._send_json(_rate_limit_intervals_list(limit))

        elif path == "/api/db/rate-limit-stats":
            self._send_json(_rate_limit_stats())

        else:
            file_path = os.path.join(DASHBOARD_DIR, path.lstrip("/"))
            if os.path.isfile(file_path):
                self._send_file(file_path)
            else:
                self.send_error(404)

    def do_POST(self):
        parsed = urlparse(self.path)
        path   = parsed.path.rstrip("/") or "/"

        if path == "/api/blacklist-track":
            body = self._read_body()
            artist_id = body.get("artist_id", "")
            track_id  = body.get("track_id", "")
            if not artist_id or not track_id:
                self._send_json({"error": "artist_id et track_id requis"}, 400)
            else:
                self._send_json(_blacklist_track_rw(artist_id, track_id))

        elif path == "/api/setup/create-config":
            body = self._read_body()
            self._send_json(_create_config_file(body))

        else:
            self.send_error(404)


# ── Server lifecycle ──────────────────────────────────────────────────────────
_server: HTTPServer | None = None


def start(port: int | None = None) -> int:
    global _server
    base_port  = port or getattr(config, "DASHBOARD_PORT", 8080)
    candidates = [base_port] + list(range(8081, 8091)) + [9090, 9191, 5500, 5501]

    for p in candidates:
        try:
            _server = HTTPServer(("127.0.0.1", p), DashboardHandler)
            t = threading.Thread(
                target=_server.serve_forever, daemon=True, name="dashboard-http"
            )
            t.start()
            logger.info("Dashboard disponible sur http://127.0.0.1:%d", p)
            return p
        except OSError as e:
            logger.warning("Port %d indisponible (%s), essai suivant…", p, e.strerror)

    logger.error("Dashboard désactivé : aucun port disponible parmi %s", candidates)
    return 0


def stop():
    global _server
    if _server:
        _server.shutdown()
        _server = None
