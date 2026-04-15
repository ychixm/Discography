import time
import requests
from urllib.parse import urlencode

from .. import config


def get_auth_code_url() -> str:
    params = {
        "client_id":     config.CLIENT_ID,
        "response_type": "code",
        "redirect_uri":  config.REDIRECT_URI,
        "scope":         " ".join(config.SCOPES),
    }
    return "https://accounts.spotify.com/authorize?" + urlencode(params)


def exchange_code_for_token(code: str) -> dict:
    r = requests.post(
        "https://accounts.spotify.com/api/token",
        data={
            "grant_type":    "authorization_code",
            "code":          code,
            "redirect_uri":  config.REDIRECT_URI,
            "client_id":     config.CLIENT_ID,
            "client_secret": config.CLIENT_SECRET,
        },
        timeout=config.REQUEST_TIMEOUT,
    )
    r.raise_for_status()
    return r.json()


def refresh_access_token(refresh_token: str) -> tuple:
    r = requests.post(
        "https://accounts.spotify.com/api/token",
        data={
            "grant_type":    "refresh_token",
            "refresh_token": refresh_token,
            "client_id":     config.CLIENT_ID,
            "client_secret": config.CLIENT_SECRET,
        },
        timeout=config.REQUEST_TIMEOUT,
    )
    r.raise_for_status()
    data = r.json()
    return data["access_token"], time.time() + data["expires_in"]
