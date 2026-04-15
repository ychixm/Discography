"""
api/client.py
=============
Client HTTP Spotify.

Changements v2 :
  - La gestion 429 lève maintenant une exception dédiée RateLimitError
    après avoir enregistré les stats, au lieu de dormir en place.
    Cela permet au appelant (main.py / run_worker) de :
      1. sauvegarder le checkpoint immédiatement
      2. attendre le Retry-After
      3. reprendre le run proprement
  - Les autres comportements (refresh token, retry réseau, 5xx) sont inchangés.
"""

import time
import random
import logging
import requests

from .. import config
from .auth import refresh_access_token
from .. import dashboard_server

logger = logging.getLogger("spotify_discography")


# ── Exception dédiée 429 ──────────────────────────────────────────────────────

class RateLimitError(Exception):
    """Levée quand Spotify répond 429 et que retry_after est connu."""
    def __init__(self, retry_after: float):
        super().__init__(f"Rate limited — retry after {retry_after:.1f}s")
        self.retry_after = retry_after


# ── Catégories d'endpoints ────────────────────────────────────────────────────
ENDPOINT_CATEGORIES = {
    "artists_albums":    ("GET /artists/{id}/albums",      lambda m, u: "/artists/" in u and "/albums" in u),
    "albums_tracks":     ("GET /albums/{id}/tracks",       lambda m, u: "/albums/" in u and "/tracks" in u),
    "playlists_items":   ("GET /playlists/{id}/items",     lambda m, u: m == "GET"  and "/playlists/" in u and "/items" in u),
    "playlists_add":     ("POST /playlists/{id}/items",    lambda m, u: m == "POST" and "/playlists/" in u and "/items" in u),
    "me_playlists":      ("GET /me/playlists",             lambda m, u: "/me/playlists" in u and m == "GET"),
    "me_playlists_post": ("POST /me/playlists",            lambda m, u: "/me/playlists" in u and m == "POST"),
    "me_following":      ("GET /me/following",             lambda m, u: "/me/following" in u),
    "me":                ("GET /me",                       lambda m, u: u.rstrip("/").endswith("/me") and m == "GET"),
    "token_refresh":     ("POST /api/token",               lambda m, u: "accounts.spotify.com" in u),
}


def _categorize(method: str, url: str) -> str:
    for key, (_, test) in ENDPOINT_CATEGORIES.items():
        try:
            if test(method, url):
                return key
        except Exception:
            pass
    return "other"


class SpotifyClient:

    def __init__(self, access_token: str, refresh_token: str, expires_at: float):
        self.access_token  = access_token
        self.refresh_token = refresh_token
        self.expires_at    = expires_at

        self._call_timestamps: list = []

        # ── Stats globales ──
        self.stats = {
            "albums":    0,
            "playlists": 0,
            "tracks":    0,
            "other":     0,
            "429":       0,
            "5xx":       0,
        }
        self.total_calls = 0

        # ── Stats par endpoint ──
        self.endpoint_stats: dict = {
            key: {
                "calls":              0,
                "429":                0,
                "label":              label,
                "last_429_ts":        None,
                "calls_at_first_429": None,
                "retry_after_last":   None,
            }
            for key, (label, _) in ENDPOINT_CATEGORIES.items()
        }
        self.endpoint_stats["other"] = {
            "calls": 0, "429": 0, "label": "Other",
            "last_429_ts": None, "calls_at_first_429": None, "retry_after_last": None,
        }

        # ── Intervalles 429 ──────────────────────────────────────────────────
        # Chaque élément est un dict décrivant un intervalle entre deux 429 :
        # {
        #   "start_ts"     : float  — timestamp de début de l'intervalle
        #                            (= fin du Retry-After précédent, ou démarrage du client)
        #   "end_ts"       : float  — timestamp du 429 qui clôt cet intervalle
        #   "ok_calls"     : int    — appels réussis dans cet intervalle
        #   "fail_calls"   : int    — appels en erreur (429 + 5xx + réseau) dans cet intervalle
        #   "retry_after"  : float  — durée Retry-After annoncée par Spotify
        #   "endpoint"     : str    — catégorie de l'endpoint qui a déclenché le 429
        # }
        # L'intervalle en cours est _current_interval (non encore clos).
        self._rate_limit_intervals: list = []
        self._current_interval: dict = self._new_interval()

        self.market: str = ""

    # ── Intervalles 429 ───────────────────────────────────────────────────────
    def _new_interval(self) -> dict:
        return {
            "start_ts":    time.time(),
            "end_ts":      None,
            "ok_calls":    0,
            "fail_calls":  0,
            "retry_after": None,
            "endpoint":    None,
        }

    def _close_interval(self, retry_after: float, endpoint: str):
        """Clôt l'intervalle courant sur un 429 et en ouvre un nouveau."""
        self._current_interval["end_ts"]      = time.time()
        self._current_interval["retry_after"] = retry_after
        self._current_interval["endpoint"]    = endpoint
        self._rate_limit_intervals.append(dict(self._current_interval))
        # Limite à 200 intervalles en mémoire
        if len(self._rate_limit_intervals) > 200:
            self._rate_limit_intervals.pop(0)

    def notify_retry_after_elapsed(self):
        """
        À appeler par main.py dès que l'attente Retry-After est terminée,
        pour démarrer un nouvel intervalle avec le bon timestamp de début.
        """
        self._current_interval = self._new_interval()
        self._push_stats_to_dashboard()

    # ── Rate limit ───────────────────────────────────────────────────────────
    def _enforce_rate_limit(self):
        while True:
            now = time.time()
            self._call_timestamps = [
                t for t in self._call_timestamps
                if now - t < config.RATE_LIMIT_WINDOW
            ]
            if len(self._call_timestamps) < config.MAX_CALLS:
                break
            oldest = self._call_timestamps[0]
            wait   = config.RATE_LIMIT_WINDOW - (now - oldest) + 0.001
            logger.info("QUOTA_WAIT window=%d/%d wait=%.3fs",
                        len(self._call_timestamps), config.MAX_CALLS, wait)
            time.sleep(wait)

    # ── Enregistrement d'un appel réussi ─────────────────────────────────────
    def _record_call(self, method: str, url: str):
        self._call_timestamps.append(time.time())
        self.total_calls += 1
        self._current_interval["ok_calls"] += 1

        if "/artists/" in url and "/albums" in url:
            self.stats["albums"] += 1
        elif "/albums/" in url and "/tracks" in url:
            self.stats["tracks"] += 1
        elif "/playlists/" in url or "/me/playlists" in url:
            self.stats["playlists"] += 1
        else:
            self.stats["other"] += 1

        cat = _categorize(method, url)
        self.endpoint_stats[cat]["calls"] += 1

        dashboard_server.record_api_call()
        self._push_stats_to_dashboard()

    # ── Enregistrement d'une erreur 429 ──────────────────────────────────────
    def _record_429(self, method: str, url: str, retry_after: float = 0.0):
        self.stats["429"] += 1
        self._current_interval["fail_calls"] += 1
        cat = _categorize(method, url)
        ep  = self.endpoint_stats[cat]
        ep["429"]             += 1
        ep["last_429_ts"]      = time.time()
        ep["retry_after_last"] = retry_after
        if ep["calls_at_first_429"] is None:
            ep["calls_at_first_429"] = ep["calls"]
        # Clôture de l'intervalle courant
        self._close_interval(retry_after=retry_after, endpoint=cat)
        self._push_stats_to_dashboard()

    def _record_5xx(self, method: str, url: str):
        self.stats["5xx"] += 1
        self._current_interval["fail_calls"] += 1
        self._push_stats_to_dashboard()

    def _push_stats_to_dashboard(self):
        # Intervalle courant (non clos) avec total pour l'affichage
        ci = dict(self._current_interval)
        ci["total_calls"] = ci["ok_calls"] + ci["fail_calls"]
        dashboard_server.update_run_state(stats={
            "api_total_calls":        self.total_calls,
            "api_detail":             dict(self.stats),
            "api_429":                self.stats["429"],
            "api_5xx":                self.stats["5xx"],
            "endpoint_stats":         {k: dict(v) for k, v in self.endpoint_stats.items()},
            "rate_limit_intervals":   list(self._rate_limit_intervals),
            "current_interval":       ci,
        })

    # ── Token refresh ─────────────────────────────────────────────────────────
    def _maybe_refresh(self):
        if time.time() > self.expires_at - 60:
            logger.info("TOKEN_REFRESH proactive")
            self.access_token, self.expires_at = refresh_access_token(self.refresh_token)

    # ── Requête principale ────────────────────────────────────────────────────
    def request(self, method: str, url: str, **kwargs) -> requests.Response:
        extra_headers = kwargs.pop("headers", {})
        attempts = 0

        while attempts < config.RETRY_MAX_ATTEMPTS:
            self._maybe_refresh()
            self._enforce_rate_limit()

            headers = {**extra_headers, "Authorization": f"Bearer {self.access_token}"}

            try:
                r = requests.request(
                    method, url,
                    headers=headers,
                    timeout=config.REQUEST_TIMEOUT,
                    **kwargs,
                )
            except requests.RequestException as e:
                wait = config.RETRY_BASE_DELAY * (2 ** attempts)
                logger.warning("NETWORK_ERROR %s retry in %.1fs", e, wait)
                time.sleep(wait)
                attempts += 1
                continue

            # ── 429 : on enregistre et on LÈVE immédiatement ─────────────────
            if r.status_code == 429:
                retry_after = float(r.headers.get("Retry-After", 60))
                self._record_429(method, url, retry_after)
                logger.warning(
                    "SPOTIFY_429 [%s %s] retry_after=%.1fs — propagation vers le run",
                    method, url, retry_after,
                )
                raise RateLimitError(retry_after)

            if r.status_code == 401:
                logger.warning("TOKEN_EXPIRED force refresh")
                self.access_token, self.expires_at = refresh_access_token(self.refresh_token)
                attempts += 1
                continue

            if 500 <= r.status_code < 600:
                self._record_5xx(method, url)
                wait = config.RETRY_BASE_DELAY * (2 ** attempts)
                logger.warning("SERVER_ERROR %d retry in %.1fs", r.status_code, wait)
                time.sleep(wait)
                attempts += 1
                continue

            r.raise_for_status()

            self._record_call(method, url)
            delay = random.uniform(config.DELAY_MIN, config.DELAY_MAX)
            time.sleep(delay)
            return r

        raise RuntimeError(f"API retry exceeded — url={url}")

    def get(self, url: str, **kwargs) -> requests.Response:
        return self.request("GET", url, **kwargs)

    def post(self, url: str, **kwargs) -> requests.Response:
        return self.request("POST", url, **kwargs)

    def paginate(self, url: str, params: dict = None) -> list:
        items = []
        first = True
        while url:
            r     = self.get(url, params=(params if first else None))
            first = False
            data  = r.json()
            items.extend(data.get("items", []))
            url = data.get("next")
        return items

    def get_me(self) -> dict:
        data = self.get(f"{config.API_BASE}/me").json()
        self.market = data.get("country", "")
        if self.market:
            logger.info("Marche utilisateur detecte : %s", self.market)
        else:
            logger.warning("Aucun pays detecte dans le profil — market non transmis")
        return data

    def get_followed_artists(self) -> list:
        url     = f"{config.API_BASE}/me/following"
        params  = {"type": "artist", "limit": config.LIMIT_FOLLOWED_ARTISTS}
        artists = []
        while True:
            data  = self.get(url, params=params).json()
            artists.extend(data["artists"]["items"])
            after = data["artists"]["cursors"]["after"]
            if not after:
                break
            params = {"type": "artist", "limit": config.LIMIT_FOLLOWED_ARTISTS, "after": after}
        logger.info("Suivi : %d artistes recuperes", len(artists))
        return artists

    def load_all_playlists(self, me_id: str) -> dict:
        playlists = self.paginate(
            f"{config.API_BASE}/me/playlists",
            params={"limit": config.LIMIT_PLAYLISTS},
        )
        mapping = {}
        for p in playlists:
            if p and p["owner"]["id"] == me_id and p["name"] not in mapping:
                mapping[p["name"]] = p["id"]
        logger.info("Playlists chargees en memoire : %d", len(mapping))
        return mapping
