#!/usr/bin/env python3
"""
launcher_linux.py
=================
Équivalent Linux de tray_launcher.pyw.

Différences avec la version Windows :
  - Verrou exclusif via fcntl.flock (POSIX) au lieu d'un fichier PID+OpenProcess.
  - Crash log dans ~/.local/share/SpotifyDiscography/ (cohérent avec APP_DATA_DIR).
  - Notification d'erreur via notify-send (libnotify) si disponible,
    avec fallback terminal.
  - Supporte le lancement en arrière-plan automatique si --daemon est passé
    en argument (double-fork POSIX).
  - Compatible avec les lanceurs .desktop (XDG).

Dépendances système optionnelles :
  - libnotify / notify-send  : notifications d'erreur visibles
  - pystray + Pillow          : icône tray (déjà requis par l'appli)

Usage :
  python3 launcher_linux.py              # premier plan (logs dans le terminal)
  python3 launcher_linux.py --daemon     # arrière-plan (double-fork)
  python3 launcher_linux.py --no-tray   # sans icône tray (server/headless)
"""

from __future__ import annotations

import atexit
import datetime
import fcntl
import os
import signal
import subprocess
import sys
import traceback

# ── Répertoire canonique du script ────────────────────────────────────────────
_HERE = os.path.dirname(os.path.realpath(os.path.abspath(__file__)))

os.chdir(_HERE)

if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

# ── Chemins XDG ───────────────────────────────────────────────────────────────
_XDG_DATA = os.environ.get(
    "XDG_DATA_HOME",
    os.path.join(os.path.expanduser("~"), ".local", "share"),
)
_APP_DATA  = os.path.join(_XDG_DATA, "SpotifyDiscography")
_RUNTIME   = os.environ.get(
    "XDG_RUNTIME_DIR",
    os.path.join("/tmp", f"spotify_discography_{os.getuid()}"),
)
os.makedirs(_APP_DATA,  exist_ok=True)
os.makedirs(_RUNTIME,   exist_ok=True)

_LOCK_PATH  = os.path.join(_RUNTIME,  "spotify_discography.lock")
_PID_PATH   = os.path.join(_RUNTIME,  "spotify_discography.pid")
_CRASH_LOG  = os.path.join(_APP_DATA, "crash.log")

# ── Fichier de verrou POSIX (fcntl) ───────────────────────────────────────────
_lock_fh = None   # garde la référence pour que le verrou reste actif


def _acquire_lock() -> bool:
    """
    Verrou exclusif non-bloquant via fcntl.LOCK_EX | fcntl.LOCK_NB.
    Retourne True si le verrou est obtenu, False si une instance tourne déjà.
    Le fichier reste ouvert pour toute la durée du processus.
    """
    global _lock_fh
    try:
        _lock_fh = open(_LOCK_PATH, "w")
        fcntl.flock(_lock_fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        _lock_fh.write(str(os.getpid()))
        _lock_fh.flush()
        atexit.register(_release_lock)
        return True
    except OSError:
        # LOCK_NB lève OSError si le verrou est déjà pris
        if _lock_fh:
            _lock_fh.close()
            _lock_fh = None
        return False


def _release_lock():
    global _lock_fh
    if _lock_fh:
        try:
            fcntl.flock(_lock_fh, fcntl.LOCK_UN)
            _lock_fh.close()
        except OSError:
            pass
        _lock_fh = None
    for p in (_LOCK_PATH, _PID_PATH):
        try:
            os.remove(p)
        except OSError:
            pass


# ── PID file (pour systemd / scripts externes) ────────────────────────────────
def _write_pid():
    try:
        with open(_PID_PATH, "w") as f:
            f.write(str(os.getpid()))
        atexit.register(lambda: _safe_remove(_PID_PATH))
    except OSError:
        pass


def _safe_remove(path: str):
    try:
        os.remove(path)
    except OSError:
        pass


# ── Journalisation des crashs ─────────────────────────────────────────────────
def _log_crash(exc: BaseException):
    try:
        with open(_CRASH_LOG, "a", encoding="utf-8") as f:
            f.write(f"\n{'='*60}\n")
            f.write(f"CRASH — {datetime.datetime.now().isoformat()}\n")
            f.write(f"PID : {os.getpid()}\n")
            f.write(traceback.format_exc())
    except OSError:
        pass


def _notify_error(title: str, body: str):
    """
    Tente d'afficher une notification d'erreur via notify-send,
    puis via une impression stderr en fallback.
    """
    try:
        subprocess.run(
            ["notify-send", "--urgency=critical", "--icon=dialog-error", title, body],
            timeout=5,
            check=False,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass
    # Fallback terminal (utile si lancé depuis un terminal ou via journald)
    print(f"[ERREUR] {title}: {body}", file=sys.stderr)


# ── Double-fork daemon (POSIX) ────────────────────────────────────────────────
def _daemonize():
    """
    Double-fork classique pour détacher le processus du terminal.
    Redirige stdin/stdout/stderr vers /dev/null.
    """
    # Premier fork
    pid = os.fork()
    if pid > 0:
        sys.exit(0)   # Père se termine

    os.setsid()       # Nouveau groupe de sessions

    # Deuxième fork (prévient la réacquisition d'un terminal de contrôle)
    pid = os.fork()
    if pid > 0:
        sys.exit(0)

    # Ferme les descripteurs standard
    sys.stdout.flush()
    sys.stderr.flush()
    devnull = os.open(os.devnull, os.O_RDWR)
    os.dup2(devnull, sys.stdin.fileno())
    os.dup2(devnull, sys.stdout.fileno())
    os.dup2(devnull, sys.stderr.fileno())
    os.close(devnull)


# ── Gestion des signaux ───────────────────────────────────────────────────────
def _setup_signals():
    """
    SIGTERM et SIGINT provoquent un arrêt propre via sys.exit(),
    ce qui déclenche les handlers atexit (relâchement du verrou, etc.).
    """
    def _graceful_exit(signum, frame):
        sys.exit(0)

    signal.signal(signal.SIGTERM, _graceful_exit)
    signal.signal(signal.SIGINT,  _graceful_exit)


# ── Point d'entrée ────────────────────────────────────────────────────────────
def main():
    args = sys.argv[1:]

    daemon_mode  = "--daemon"   in args
    no_tray_mode = "--no-tray"  in args

    # Mode daemon : détacher du terminal avant d'acquérir le verrou
    if daemon_mode:
        _daemonize()

    _setup_signals()

    # Vérification de l'instance unique
    if not _acquire_lock():
        _notify_error(
            "Spotify Discography",
            "Une instance est déjà en cours d'exécution.\n"
            f"Verrou : {_LOCK_PATH}",
        )
        sys.exit(0)

    _write_pid()

    # Mode headless : désactive pystray avant l'import de main
    if no_tray_mode:
        os.environ["SPOTIFY_DISCOGRAPHY_NO_TRAY"] = "1"

    try:
        from spotify_discography.main import main as _app_main
        _app_main()
    except SystemExit:
        raise
    except BaseException as exc:
        _log_crash(exc)
        _notify_error(
            "Spotify Discography — Erreur fatale",
            f"{type(exc).__name__}: {exc}\n\nDétails : {_CRASH_LOG}",
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
