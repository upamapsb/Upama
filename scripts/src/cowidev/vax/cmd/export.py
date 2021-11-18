import webbrowser
import pyperclip
from cowidev.vax.cmd.utils import get_logger
from cowidev.megafile.generate import generate_megafile

logger = get_logger()


def main_export(paths, url):
    main_source_table_html(paths, url)
    main_megafile()


def main_source_table_html(paths, url):
    # Read html content
    print("-- Reading HTML table... --")
    with open(paths.tmp_html, "r") as f:
        html = f.read()
    logger.info("Redirecting to owid editing platform...")
    pyperclip.copy(html)
    webbrowser.open(url)


def main_megafile():
    """Executes scripts/scripts/megafile.py."""
    print("-- Generating megafiles... --")
    generate_megafile()
