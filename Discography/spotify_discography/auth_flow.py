"""
auth_flow.py
============
Gestion complète du flow OAuth2 Spotify.

Chiffrement :
   AES-GCM via `cryptography` (pip install cryptography).
   Si absent, AVERTISSEMENT clair + fallback XOR (moins sécurisé).
   La clé n'est jamais stockée dans le fichier de config.
"""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import os
import platform
import secrets
import socket
import threading
import time
import uuid
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Optional
from urllib.parse import parse_qs, urlencode, urlparse

import requests

from . import config

logger = logging.getLogger("spotify_discography")

# ── Clés de stockage dans config.json ────────────────────────────────────────
_KEY_ACCESS   = "token_access"
_KEY_REFRESH  = "token_refresh"
_KEY_EXPIRES  = "token_expires_at"

# ── Durée max d'attente du callback navigateur ────────────────────────────────
_CALLBACK_TIMEOUT = 120   # secondes

# ── Vérification AES au chargement du module ─────────────────────────────────
_AES_AVAILABLE = False
try:
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM  # noqa: F401
    _AES_AVAILABLE = True
except ImportError:
    pass

def check_encryption_warning(notify_callback=None):
    """
    Appelé depuis main.py après l'init du tray.
    Logue un avertissement visible et notifie le tray si AES est absent.
    """
    if not _AES_AVAILABLE:
        msg = (
            "SECURITE : chiffrement AES indisponible — tokens stockes en XOR (faible). "
            "Installez : pip install cryptography"
        )
        logger.warning(msg)
        if notify_callback:
            notify_callback(
                "Sécurité — chiffrement faible",
                "pip install cryptography  →  pour activer AES-GCM"
            )


# ══════════════════════════════════════════════════════════════════════════════
#  CHIFFREMENT
# ══════════════════════════════════════════════════════════════════════════════

def _machine_key() -> bytes:
    parts = [
        _get_machine_id(),
        os.environ.get("USERNAME", os.environ.get("USER", "user")),
        platform.node(),
        "spotify_discography_v1",
    ]
    raw = "|".join(parts).encode()
    return hashlib.sha256(raw).digest()


def _get_machine_id() -> str:
    if platform.system() == "Windows":
        try:
            import winreg
            key = winreg.OpenKey(
                winreg.HKEY_LOCAL_MACHINE,
                r"SOFTWARE\Microsoft\Cryptography",
            )
            value, _ = winreg.QueryValueEx(key, "MachineGuid")
            winreg.CloseKey(key)
            return value
        except Exception:
            pass
    for path in ("/etc/machine-id", "/var/lib/dbus/machine-id"):
        if os.path.exists(path):
            with open(path) as f:
                return f.read().strip()
    return str(uuid.getnode())


def _encrypt(plaintext: str) -> str:
    key = _machine_key()
    if _AES_AVAILABLE:
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM
        nonce      = secrets.token_bytes(12)
        ciphertext = AESGCM(key).encrypt(nonce, plaintext.encode(), None)
        payload    = nonce + ciphertext
        return "aes:" + base64.b64encode(payload).decode()
    else:
        # Fallback XOR — avertissement déjà émis par check_encryption_warning()
        data = plaintext.encode()
        key_stream = (key * (len(data) // len(key) + 1))[:len(data)]
        xored = bytes(a ^ b for a, b in zip(data, key_stream))
        return "xor:" + base64.b64encode(xored).decode()


def _decrypt(ciphertext: str) -> str:
    key = _machine_key()
    if ciphertext.startswith("aes:"):
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM
        payload = base64.b64decode(ciphertext[4:])
        nonce   = payload[:12]
        ct      = payload[12:]
        return AESGCM(key).decrypt(nonce, ct, None).decode()
    elif ciphertext.startswith("xor:"):
        data = base64.b64decode(ciphertext[4:])
        key_stream = (key * (len(data) // len(key) + 1))[:len(data)]
        return bytes(a ^ b for a, b in zip(data, key_stream)).decode()
    else:
        return ciphertext   # migration depuis ancienne version non chiffrée


# ══════════════════════════════════════════════════════════════════════════════
#  STOCKAGE DES TOKENS
# ══════════════════════════════════════════════════════════════════════════════

def _load_config_raw() -> dict:
    config_path = os.environ.get("SPOTIFY_CONFIG_PATH", "config.json")
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)


def _save_config_raw(data: dict):
    config_path = os.environ.get("SPOTIFY_CONFIG_PATH", "config.json")
    tmp = config_path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    os.replace(tmp, config_path)


def save_tokens(access_token: str, refresh_token: str, expires_at: float):
    try:
        data = _load_config_raw()
        data[_KEY_ACCESS]  = _encrypt(access_token)
        data[_KEY_REFRESH] = _encrypt(refresh_token)
        data[_KEY_EXPIRES] = expires_at
        _save_config_raw(data)
        enc_label = "AES-GCM" if _AES_AVAILABLE else "XOR (faible — pip install cryptography)"
        logger.info("Tokens sauvegardes dans config.json (%s)", enc_label)
    except Exception as e:
        logger.error("Impossible de sauvegarder les tokens : %s", e)


def load_tokens() -> Optional[tuple[str, str, float]]:
    try:
        data = _load_config_raw()
        if _KEY_REFRESH not in data:
            return None
        access  = _decrypt(data[_KEY_ACCESS])
        refresh = _decrypt(data[_KEY_REFRESH])
        expires = float(data.get(_KEY_EXPIRES, 0))
        return access, refresh, expires
    except Exception as e:
        logger.warning("Impossible de charger les tokens : %s", e)
        return None


def clear_tokens():
    try:
        data = _load_config_raw()
        for key in (_KEY_ACCESS, _KEY_REFRESH, _KEY_EXPIRES):
            data.pop(key, None)
        _save_config_raw(data)
        logger.info("Tokens supprimes de config.json")
    except Exception as e:
        logger.error("Impossible de supprimer les tokens : %s", e)


# ══════════════════════════════════════════════════════════════════════════════
#  SERVEUR DE CALLBACK LOCAL
# ══════════════════════════════════════════════════════════════════════════════

_HTML_SUCCESS = """<!DOCTYPE html>
<html lang="fr">
<head>
<meta charset="UTF-8">
<title>Spotify Discography — Authentification</title>
<style>
  body { font-family: monospace; background: #0a0a0b; color: #e2e2ec;
         display: flex; align-items: center; justify-content: center;
         height: 100vh; margin: 0; }
  .box { text-align: center; }
  .check { font-size: 64px; color: #1DB954; }
  h1 { color: #1DB954; font-size: 22px; margin: 16px 0 8px; }
  p  { color: #8888aa; font-size: 14px; }
</style>
</head>
<body>
  <div class="box">
    <div class="check">✓</div>
    <h1>Authentification réussie</h1>
    <p>Vous pouvez fermer cet onglet.<br>Le scan démarre automatiquement.</p>
  </div>
</body>
</html>"""

_HTML_ERROR = """<!DOCTYPE html>
<html lang="fr">
<head><meta charset="UTF-8"><title>Erreur</title>
<style>body{font-family:monospace;background:#0a0a0b;color:#ff4545;
display:flex;align-items:center;justify-content:center;height:100vh;margin:0;}
</style></head>
<body><div style="text-align:center">
  <div style="font-size:64px">✗</div>
  <h1>Erreur d'authentification</h1>
  <p style="color:#8888aa">Vérifiez les logs de l'application.</p>
</div></body></html>"""


class _CallbackHandler(BaseHTTPRequestHandler):

    def log_message(self, fmt, *args):
        pass

    def do_GET(self):
        parsed = urlparse(self.path)
        qs    = parse_qs(parsed.query)
        code  = qs.get("code",  [None])[0]
        error = qs.get("error", [None])[0]

        if not code and not error:
            self.send_error(404)
            return

        if code:
            self.server._auth_code  = code
            self.server._auth_error = None
            body = _HTML_SUCCESS.encode()
        else:
            self.server._auth_code  = None
            self.server._auth_error = error or "unknown_error"
            body = _HTML_ERROR.encode()

        self.send_response(200)
        self.send_header("Content-Type",   "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection",     "close")
        self.end_headers()
        self.wfile.write(body)

        threading.Thread(target=self.server.shutdown, daemon=True).start()


def _find_free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _extract_redirect_port() -> int:
    try:
        parsed = urlparse(config.REDIRECT_URI)
        if parsed.port:
            return parsed.port
    except Exception:
        pass
    logger.warning(
        "Impossible de lire le port depuis REDIRECT_URI=%s — port aleatoire utilisé.",
        config.REDIRECT_URI,
    )
    return _find_free_port()


# ══════════════════════════════════════════════════════════════════════════════
#  FLOW PRINCIPAL
# ══════════════════════════════════════════════════════════════════════════════

def get_auth_url(state: str) -> str:
    params = {
        "client_id":     config.CLIENT_ID,
        "response_type": "code",
        "redirect_uri":  config.REDIRECT_URI,
        "scope":         " ".join(config.SCOPES),
        "state":         state,
    }
    return "https://accounts.spotify.com/authorize?" + urlencode(params)


def _exchange_code(code: str) -> dict:
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


def do_auth_flow() -> tuple[str, str, float]:
    port  = _extract_redirect_port()
    state = secrets.token_urlsafe(16)

    server = HTTPServer(("127.0.0.1", port), _CallbackHandler)
    server._auth_code  = None
    server._auth_error = None

    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()

    auth_url = get_auth_url(state)
    logger.info("Ouverture du navigateur pour l'authentification Spotify")
    webbrowser.open(auth_url)

    deadline = time.time() + _CALLBACK_TIMEOUT
    while server._auth_code is None and server._auth_error is None:
        if time.time() > deadline:
            server.shutdown()
            raise RuntimeError(
                f"Timeout : aucun callback reçu en {_CALLBACK_TIMEOUT}s. "
                "Vérifiez que REDIRECT_URI dans config.json pointe vers "
                f"http://127.0.0.1:{port}/callback"
            )
        time.sleep(0.2)

    if server._auth_error:
        raise RuntimeError(f"Spotify a retourné une erreur : {server._auth_error}")

    code = server._auth_code
    logger.info("Code d'autorisation reçu — échange en cours…")

    data       = _exchange_code(code)
    access     = data["access_token"]
    refresh    = data["refresh_token"]
    expires_at = time.time() + data["expires_in"]

    save_tokens(access, refresh, expires_at)
    logger.info("Authentification réussie — tokens sauvegardés")

    return access, refresh, expires_at


def ensure_authenticated() -> tuple[str, str, float]:
    tokens = load_tokens()

    if tokens:
        access, refresh, expires_at = tokens

        if time.time() < expires_at - 60:
            logger.info("Tokens existants valides — pas d'authentification nécessaire")
            return access, refresh, expires_at

        logger.info("Access token expiré — refresh silencieux…")
        try:
            from .api.auth import refresh_access_token
            new_access, new_expires = refresh_access_token(refresh)
            save_tokens(new_access, refresh, new_expires)
            logger.info("Refresh silencieux réussi")
            return new_access, refresh, new_expires
        except Exception as e:
            logger.warning("Refresh échoué (%s) — re-authentification complète", e)
            clear_tokens()

    logger.info("Aucun token valide — lancement du flow d'authentification")
    return do_auth_flow()
