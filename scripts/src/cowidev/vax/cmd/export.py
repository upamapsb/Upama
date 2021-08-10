import os
import importlib.util
import webbrowser
import pyperclip
from cowidev.vax.cmd.utils import get_logger


logger = get_logger()


def main_export(paths, url):
    main_source_table_html(paths, url)
    main_megafile(paths)


def main_source_table_html(paths, url):
    # Read html content
    print("-- Reading HTML table... --")
    with open(paths.tmp_html, "r") as f:
        html = f.read()
    logger.info("Redirecting to owid editing platform...")
    pyperclip.copy(html)
    webbrowser.open(url)


def main_megafile(paths):
    """Executes scripts/scripts/megafile.py."""
    print("-- Generating megafiles... --")
    script_path = os.path.join(paths.tmp_tmp, "megafile.py")
    spec = importlib.util.spec_from_file_location("megafile", script_path)
    megafile = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(megafile)
    megafile.generate_megafile()
