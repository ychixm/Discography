"""
tray_icon.py
============
Icône system tray avec animation pendant le scan.
L'animation alterne des frames générées en mémoire par Pillow —
aucune dépendance supplémentaire (pystray + Pillow suffisent).

Principe :
  - Idle / done  : icône Spotify statique.
  - Running      : rotation de la couleur de fond entre vert foncé et vert
                   vif + point tournant, via un thread de mise à jour toutes
                   les 500 ms.  pystray.Icon.icon est mis à jour en place.
"""

from __future__ import annotations

import math
import threading
import time
import webbrowser
import logging
from typing import Callable

logger = logging.getLogger("spotify_discography")

try:
    import pystray
    from PIL import Image, ImageDraw
    _AVAILABLE = True
except ImportError:
    _AVAILABLE = False
    logger.warning(
        "pystray / Pillow non installés — icône tray désactivée. "
        "Installez-les avec : pip install pystray pillow"
    )


# ══════════════════════════════════════════════════════════════════════════════
#  GÉNÉRATION DES FRAMES
# ══════════════════════════════════════════════════════════════════════════════

_GREEN_DARK  = (18, 130, 60, 255)
_GREEN_LIGHT = (29, 185, 84, 255)
_WHITE       = (255, 255, 255, 255)


def _draw_spotify_waves(draw: "ImageDraw.Draw", cx: int, cy: int, size: int):
    """Dessine les trois arcs blancs caractéristiques du logo Spotify."""
    for r_pct, y_off_pct, w_pct in [
        (0.62, -0.12, 0.09),
        (0.42,  0.04, 0.08),
        (0.22,  0.18, 0.07),
    ]:
        r     = int(size * r_pct)
        y_off = int(size * y_off_pct)
        w     = max(2, int(size * w_pct))
        bbox  = [cx - r, cy + y_off - r, cx + r, cy + y_off + r]
        draw.arc(bbox, start=210, end=330, fill=_WHITE, width=w)


def _make_static_frame(size: int = 64) -> "Image.Image":
    img  = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)
    draw.ellipse([0, 0, size - 1, size - 1], fill=_GREEN_LIGHT)
    _draw_spotify_waves(draw, size // 2, size // 2, size)
    return img


def _make_animated_frame(phase: float, size: int = 64) -> "Image.Image":
    """
    Génère une frame d'animation.
    phase ∈ [0, 1) — détermine la couleur de fond et la position d'un point.
    """
    img  = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)

    # Couleur de fond pulsante
    t = (math.sin(phase * 2 * math.pi) + 1) / 2   # ∈ [0,1]
    bg = tuple(
        int(_GREEN_DARK[i] + (_GREEN_LIGHT[i] - _GREEN_DARK[i]) * t)
        for i in range(3)
    ) + (255,)
    draw.ellipse([0, 0, size - 1, size - 1], fill=bg)

    _draw_spotify_waves(draw, size // 2, size // 2, size)

    # Point tournant (indicateur d'activité)
    angle  = phase * 2 * math.pi
    radius = size * 0.38
    cx, cy = size // 2, size // 2
    px = int(cx + radius * math.cos(angle))
    py = int(cy + radius * math.sin(angle))
    dot_r = max(2, size // 14)
    draw.ellipse([px - dot_r, py - dot_r, px + dot_r, py + dot_r], fill=_WHITE)

    return img


# Pré-calcule 8 frames pour l'animation (économie CPU)
_ANIM_FRAMES: list = []

def _precompute_frames(size: int = 64):
    global _ANIM_FRAMES
    if not _AVAILABLE:
        return
    n = 8
    _ANIM_FRAMES = [_make_animated_frame(i / n, size) for i in range(n)]

if _AVAILABLE:
    _precompute_frames()


# ══════════════════════════════════════════════════════════════════════════════
#  TRAY MANAGER
# ══════════════════════════════════════════════════════════════════════════════

class TrayIcon:

    def __init__(self, port: int, quit_callback: Callable[[], None]):
        self._port          = port
        self._quit_callback = quit_callback
        self._icon: "pystray.Icon | None" = None
        self._status        = "Initialisation…"
        self._animating     = False
        self._anim_thread: threading.Thread | None = None
        self._anim_stop     = threading.Event()

    # ── API publique ──────────────────────────────────────────────────────────

    def set_status(self, text: str):
        self._status = text
        if self._icon:
            self._icon.title = f"Spotify Discography\n{text}"
            self._rebuild_menu()

    def set_running(self, running: bool):
        """Active/désactive l'animation de l'icône."""
        if running and not self._animating:
            self._start_animation()
        elif not running and self._animating:
            self._stop_animation()

    def notify(self, title: str, message: str):
        if self._icon:
            self._icon.notify(message, title)

    def run(self):
        if not _AVAILABLE:
            import time as _time
            try:
                while True:
                    _time.sleep(60)
            except KeyboardInterrupt:
                self._quit_callback()
            return

        image = _make_static_frame(64)
        self._icon = pystray.Icon(
            name  = "spotify_discography",
            icon  = image,
            title = "Spotify Discography\nInitialisation…",
            menu  = self._build_menu(),
        )
        self._icon.run()

    def stop(self):
        self._stop_animation()
        if self._icon:
            self._icon.stop()

    # ── Animation ─────────────────────────────────────────────────────────────

    def _start_animation(self):
        if not _AVAILABLE or not _ANIM_FRAMES:
            return
        self._animating = True
        self._anim_stop.clear()
        self._anim_thread = threading.Thread(
            target=self._animation_loop, daemon=True, name="tray-anim"
        )
        self._anim_thread.start()

    def _stop_animation(self):
        self._animating = False
        self._anim_stop.set()
        if self._anim_thread:
            self._anim_thread.join(timeout=2)
            self._anim_thread = None
        # Remet l'icône statique
        if self._icon and _AVAILABLE:
            self._icon.icon = _make_static_frame(64)

    def _animation_loop(self):
        frame_idx = 0
        while not self._anim_stop.is_set():
            if self._icon and _ANIM_FRAMES:
                self._icon.icon = _ANIM_FRAMES[frame_idx % len(_ANIM_FRAMES)]
            frame_idx += 1
            self._anim_stop.wait(timeout=0.5)

    # ── Menu ──────────────────────────────────────────────────────────────────

    def _build_menu(self) -> "pystray.Menu":
        return pystray.Menu(
            pystray.MenuItem(
                "Ouvrir le dashboard",
                self._open_browser,
                default=True,
            ),
            pystray.Menu.SEPARATOR,
            pystray.MenuItem(
                lambda _: f"Statut : {self._status}",
                None,
                enabled=False,
            ),
            pystray.Menu.SEPARATOR,
            pystray.MenuItem("Quitter", self._on_quit),
        )

    def _rebuild_menu(self):
        if self._icon:
            self._icon.menu = self._build_menu()

    def _open_browser(self, icon=None, item=None):
        webbrowser.open(f"http://127.0.0.1:{self._port}")

    def _on_quit(self, icon=None, item=None):
        self._quit_callback()
        self.stop()


# ── Factory ───────────────────────────────────────────────────────────────────

def create(port: int, quit_callback: Callable[[], None]) -> TrayIcon:
    return TrayIcon(port, quit_callback)


def is_available() -> bool:
    return _AVAILABLE
