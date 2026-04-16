import json
import os
import platform


# ── Répertoire de données applicatives (AppData / ~/.local/share) ─────────────
def _app_data_dir(app_name: str = "SpotifyDiscography") -> str:
    system = platform.system()
    if system == "Windows":
        base = os.environ.get("APPDATA") or os.path.expanduser("~")
    elif system == "Darwin":
        base = os.path.join(os.path.expanduser("~"), "Library", "Application Support")
    else:
        base = os.environ.get("XDG_DATA_HOME") or os.path.join(os.path.expanduser("~"), ".local", "share")
    path = os.path.join(base, app_name)
    os.makedirs(path, exist_ok=True)
    return path


APP_DATA_DIR: str = _app_data_dir()

# ── Emplacement de config.json ────────────────────────────────────────────────
_CONFIG_PATH: str = os.environ.get(
    "SPOTIFY_CONFIG_PATH",
    os.path.join(APP_DATA_DIR, "config.json"),
)

try:
    with open(_CONFIG_PATH, "r", encoding="utf-8") as _f:
        _RAW = json.load(_f)
except FileNotFoundError:
    _RAW = {}

# OAuth — vides si config absent (flow setup en cours)
CLIENT_ID: str     = _RAW.get("client_id", "")
CLIENT_SECRET: str = _RAW.get("client_secret", "")
REDIRECT_URI: str  = _RAW.get("redirect_uri", "http://127.0.0.1:8888/callback")

SCOPES = [
    "user-read-private",
    "user-follow-read",
    "playlist-modify-public",
    "playlist-modify-private",
    "playlist-read-private",
    "playlist-read-collaborative",
]

# API
API_BASE             = "https://api.spotify.com/v1"
REQUEST_TIMEOUT: int = _RAW.get("request_timeout_seconds", 10)

# Pagination
LIMIT_PLAYLISTS: int        = 50
LIMIT_ALBUMS: int           = 10
LIMIT_ALBUM_TRACKS: int     = 50
LIMIT_PLAYLIST_ITEMS: int   = 100
LIMIT_FOLLOWED_ARTISTS: int = 50

# Rate limiting
RATE_LIMIT_WINDOW: float = _RAW.get("rate_limit_window_seconds", 30.0)
MAX_CALLS: int           = _RAW.get("rate_limit_max_calls", 30)
DELAY_MIN: float         = _RAW.get("min_request_interval_seconds", 0.1)
DELAY_MAX: float         = _RAW.get("max_request_interval_seconds", 0.4)

# Retry
RETRY_MAX_ATTEMPTS: int = _RAW.get("retry_max_attempts", 5)
RETRY_BASE_DELAY: float = _RAW.get("retry_base_delay_seconds", 1.0)

# Comportement
DELAY_BETWEEN_ARTISTS: float = _RAW.get("delay_between_artists_seconds", 1.0)
INCLUDE_GROUPS: str          = _RAW.get("include_groups", "album,single,compilation,appears_on")

# Stockage — chemins dans APP_DATA_DIR par défaut.
STATE_DB_PATH: str = _RAW.get(
    "state_db_path",
    os.path.join(APP_DATA_DIR, "state.db"),
)
LOG_FILE_PATH: str = _RAW.get(
    "log_file_path",
    os.path.join(APP_DATA_DIR, "spotify_discography.log"),
)
TOKENS_PATH: str = _RAW.get(
    "tokens_path",
    os.path.join(APP_DATA_DIR, "tokens.json"),
)

# Run
VERBOSE_LOGGING: bool    = _RAW.get("verbose_logging", False)
FULL_RESYNC_MODE: bool   = _RAW.get("full_resync_mode", False)
MAX_ARTISTS_PER_RUN: int = _RAW.get("max_artists_per_run", 0)
MARKET: str              = _RAW.get("market", "FR")

# ── Mode daemon ───────────────────────────────────────────────────────────────
# SCAN_INTERVAL : durée minimale entre deux scans d'un même artiste.
# Un album dont last_checked est plus récent que SCAN_INTERVAL est ignoré
# (économie d'appels API) sauf si FULL_RESYNC_MODE est activé.
SCAN_INTERVAL: int = _RAW.get("scan_interval_seconds", 7 * 24 * 3600)

# CYCLE_MIN_INTERVAL : attente minimale entre deux cycles complets.
# Évite un spin tight quand tous les artistes sont récents.
CYCLE_MIN_INTERVAL: int = _RAW.get("cycle_min_interval_seconds", 300)

# Dashboard
DASHBOARD_PORT: int = _RAW.get("dashboard_port", 8080)
