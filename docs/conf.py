"""Sphinx configuration.

Каркас как у sphinx-quickstart и у
https://github.com/hewimetall/dramatiq_prometeus_middware :
tutorial, how-to, reference (что), explanation (почему).
https://diataxis.fr/
"""

from __future__ import annotations

project = "queue-wait"
author = "queue-wait"
copyright = "2026, queue-wait"
release = "0.1.0"
version = release

extensions = ["myst_parser"]
source_suffix = {".md": "markdown"}
myst_heading_anchors = 3
myst_enable_extensions = ["colon_fence", "deflist"]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store", "requirements.txt"]

language = "ru"
html_theme = "alabaster"
html_static_path = ["_static"]
html_title = "Как воркер ждёт задачу"
root_doc = "index"
