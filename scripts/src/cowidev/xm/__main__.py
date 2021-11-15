import os

from cowidev.utils.utils import get_project_dir
from .etl import run_etl
from .grapher import run_explorerizer
from ._parser import _parse_args


FILE_DS = os.path.join(
    get_project_dir(), "public", "data", "excess_mortality", "excess_mortality.csv"
)
FILE_EXPLORER = os.path.join(
    get_project_dir(), "public", "data", "internal", "megafile--excess-mortality.json"
)


def run_step(step: str):
    if step == "etl":
        run_etl(FILE_DS)
    elif step == "explorer-file":
        run_explorerizer(FILE_DS, FILE_EXPLORER)


if __name__ == "__main__":
    args = _parse_args()
    run_step(args.step)
