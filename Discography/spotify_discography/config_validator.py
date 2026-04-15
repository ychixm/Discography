"""
config_validator.py
===================
Validation de config.json au démarrage.

Vérifie que tous les champs obligatoires sont présents et cohérents
avant de lancer quoi que ce soit.  En cas d'erreur, lève une
ConfigurationError avec un message clair et humain.

Appelé depuis main.py avant toute initialisation.
"""
from __future__ import annotations

import json
import os
import re
from urllib.parse import urlparse
from . import config as _cfg

class ConfigurationError(Exception):
    """Erreur de configuration — affichée dans la tray et dans les logs."""
    pass


# ── Champs obligatoires ───────────────────────────────────────────────────────
_REQUIRED_FIELDS = {
    "client_id":     "L'identifiant OAuth de votre application Spotify Dashboard",
    "client_secret": "Le secret OAuth de votre application Spotify Dashboard",
    "redirect_uri":  "L'URI de redirection enregistrée dans Spotify Dashboard (ex: http://127.0.0.1:8888/callback)",
}

# client_id et client_secret Spotify sont des chaînes hex de 32 caractères
_CLIENT_ID_RE     = re.compile(r"^[0-9a-f]{32}$", re.IGNORECASE)
_CLIENT_SECRET_RE = re.compile(r"^[0-9a-f]{32}$", re.IGNORECASE)


def validate(config_path: str | None = None) -> dict:
    """
    Charge et valide config.json.

    Retourne le dict brut si tout est valide.
    Lève ConfigurationError avec un message détaillé sinon.
    """
    path = config_path or os.environ.get("SPOTIFY_CONFIG_PATH", _cfg._CONFIG_PATH)

    # ── Existence du fichier ──────────────────────────────────────────────────
    if not os.path.exists(path):
        raise ConfigurationError(
            f"Fichier de configuration introuvable : {path}\n"
            "Créez un fichier config.json à côté de tray_launcher.pyw.\n"
            "Modèle minimal :\n"
            '{\n'
            '  "client_id":     "votre_client_id",\n'
            '  "client_secret": "votre_client_secret",\n'
            '  "redirect_uri":  "http://127.0.0.1:8888/callback"\n'
            '}'
        )

    # ── Parsing JSON ──────────────────────────────────────────────────────────
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
    except json.JSONDecodeError as e:
        raise ConfigurationError(
            f"config.json contient du JSON invalide : {e}\n"
            "Utilisez un validateur JSON (ex: jsonlint.com) pour corriger le fichier."
        )

    errors: list[str] = []

    # ── Champs obligatoires ───────────────────────────────────────────────────
    for field, description in _REQUIRED_FIELDS.items():
        value = raw.get(field, "")
        if not value or not str(value).strip():
            errors.append(
                f"• Champ manquant ou vide : \"{field}\"\n"
                f"  → {description}"
            )

    if errors:
        raise ConfigurationError(
            "config.json incomplet — champs manquants :\n\n" +
            "\n".join(errors) +
            "\n\nRetrouvez ces valeurs sur https://developer.spotify.com/dashboard"
        )

    # ── Validation du format client_id ───────────────────────────────────────
    client_id = str(raw["client_id"]).strip()
    if not _CLIENT_ID_RE.match(client_id):
        errors.append(
            f"• \"client_id\" invalide : \"{client_id}\"\n"
            "  → Doit être une chaîne hexadécimale de 32 caractères "
            "(ex: a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4)"
        )

    # ── Validation du format client_secret ───────────────────────────────────
    client_secret = str(raw["client_secret"]).strip()
    if not _CLIENT_SECRET_RE.match(client_secret):
        errors.append(
            f"• \"client_secret\" invalide : \"{client_secret[:8]}…\"\n"
            "  → Doit être une chaîne hexadécimale de 32 caractères"
        )

    # ── Validation de redirect_uri ────────────────────────────────────────────
    redirect_uri = str(raw["redirect_uri"]).strip()
    try:
        parsed = urlparse(redirect_uri)
        if parsed.scheme not in ("http", "https"):
            errors.append(
                f"• \"redirect_uri\" invalide : schéma \"{parsed.scheme}\" non supporté\n"
                "  → Utilisez http://127.0.0.1:<port>/callback"
            )
        elif not parsed.netloc:
            errors.append(
                "• \"redirect_uri\" invalide : hôte manquant\n"
                "  → Exemple : http://127.0.0.1:8888/callback"
            )
        elif parsed.port is None:
            errors.append(
                f"• \"redirect_uri\" sans port explicite : \"{redirect_uri}\"\n"
                "  → Le port est requis pour le serveur de callback local\n"
                "  → Exemple : http://127.0.0.1:8888/callback"
            )
    except Exception as e:
        errors.append(f"• \"redirect_uri\" invalide : {e}")

    # ── Vérification doublons client_id == client_secret ─────────────────────
    if client_id == client_secret and _CLIENT_ID_RE.match(client_id):
        errors.append(
            "• \"client_id\" et \"client_secret\" sont identiques\n"
            "  → Ce sont deux valeurs distinctes dans le Spotify Dashboard"
        )

    if errors:
        raise ConfigurationError(
            "config.json contient des valeurs invalides :\n\n" +
            "\n".join(errors) +
            "\n\nRetrouvez ces valeurs sur https://developer.spotify.com/dashboard"
        )

    return raw
