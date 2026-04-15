"""
tray_launcher.pyw
=================
Point d'entrée sans console Windows.
"""

import sys
import os

# Dossier contenant ce fichier
_HERE = os.path.dirname(os.path.abspath(__file__))

# S'assure que le dossier courant est dans le path
sys.path.insert(0, _HERE)

# Force le répertoire de travail vers le dossier du script
# (nécessaire lors d'un double-clic : Windows utilise un cwd différent)
os.chdir(_HERE)

from spotify_discography.main import main  # noqa

if __name__ == "__main__":
    main()