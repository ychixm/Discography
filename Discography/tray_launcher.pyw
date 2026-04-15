"""
tray_launcher.pyw
=================
Point d'entrée sans console Windows.

Si config.json est absent dans APP_DATA_DIR, ouvre le dashboard sur
/setup dans le navigateur et attend que le fichier soit créé avant
de lancer le daemon normal.
"""

import sys
import os
import time
import webbrowser
import threading
import platform

# Dossier contenant ce fichier
_HERE = os.path.dirname(os.path.abspath(__file__))

sys.path.insert(0, _HERE)
os.chdir(_HERE)


def _app_data_dir() -> str:
    """Même logique que config._app_data_dir — dupliquée ici pour éviter
    d'importer config avant de savoir si le fichier existe."""
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


def _config_path() -> str:
    return os.environ.get(
        "SPOTIFY_CONFIG_PATH",
        os.path.join(_app_data_dir(), "config.json"),
    )


def _wait_for_config_then_launch(port: int):
    """
    Tourne dans un thread secondaire.
    Scrute l'apparition de config.json dans APP_DATA_DIR,
    puis recharge le module config et démarre le daemon worker
    sans redémarrer le serveur HTTP.
    """
    cfg = _config_path()
    while not os.path.exists(cfg):
        time.sleep(0.5)

    # Petit délai pour laisser le temps à create-config de terminer l'écriture
    time.sleep(0.5)

    # Recharge config maintenant que config.json existe
    import importlib
    from spotify_discography import config as _cfg
    importlib.reload(_cfg)

    from spotify_discography.main import _daemon_worker
    _daemon_worker(port)


def _setup_flow():
    """
    Démarre uniquement le dashboard HTTP, ouvre /setup dans le
    navigateur et attend la création de config.json avant de lancer
    le daemon complet.
    """
    from spotify_discography import dashboard_server
    from spotify_discography import tray_icon

    port = dashboard_server.start()

    # Ouvre /setup dans le navigateur après un court délai
    threading.Timer(
        0.8, webbrowser.open, args=(f"http://127.0.0.1:{port}/setup",)
    ).start()

    # Thread qui surveille l'apparition de config.json
    threading.Thread(
        target=_wait_for_config_then_launch,
        args=(port,),
        daemon=True,
        name="config-watcher",
    ).start()

    def _quit():
        dashboard_server.stop()
        sys.exit(0)

    tray = tray_icon.create(port, quit_callback=_quit)
    tray.set_status("Configuration initiale — ouvrez le dashboard")
    tray.run()


if __name__ == "__main__":
    if not os.path.exists(_config_path()):
        _setup_flow()
    else:
        from spotify_discography.main import main
        main()
