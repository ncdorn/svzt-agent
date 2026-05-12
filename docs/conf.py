import importlib.metadata
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.abspath("../src"))

project = "svzt-agent"
copyright = f"Copyright © {datetime.now().year} Nick Dorn"
html_show_sphinx = False

try:
    version = importlib.metadata.version("svzt-agent")
except importlib.metadata.PackageNotFoundError:
    version = "0.0.0"

extensions = [
    "myst_parser",
    "sphinx_design",
    "sphinx_copybutton",
    "sphinx.ext.intersphinx",
    "sphinx.ext.napoleon",
    "sphinx.ext.todo",
    "autoapi.extension",
]

source_suffix = [".rst", ".md"]
master_doc = "index"
pygments_style = "default"
language = "en"

myst_enable_extensions = [
    "attrs_inline",
    "colon_fence",
    "deflist",
    "html_image",
]
myst_heading_anchors = 3
myst_footnote_transition = False

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

autoapi_type = "python"
autoapi_dirs = ["../src/svztagent"]
autoapi_add_toctree = False
autoapi_keep_files = False
autoapi_options = ["members", "undoc-members", "show-inheritance"]

html_theme = "pydata_sphinx_theme"
htmlhelp_basename = "svztagentdoc"

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
}
