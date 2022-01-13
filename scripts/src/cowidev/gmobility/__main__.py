import os

from cowidev.utils.utils import get_project_dir
from cowidev.gmobility.etl import run_etl
from cowidev.gmobility.grapher import run_grapheriser, run_db_updater
from cowidev.gmobility._parser import _parse_args


project_dir = get_project_dir()
FILE_DS = os.path.join("/tmp", "google-mobility.csv")
FILE_GRAPHER = os.path.join(project_dir, "scripts", "grapher", "Google Mobility Trends (2020).csv")
FILE_COUNTRY_STD = os.path.join(project_dir, "scripts", "input", "gmobility", "gmobility_country_standardized.csv")


def run_step(step: str):
    if step == "etl":
        run_etl(FILE_DS)
    elif step == "grapher-file":
        run_grapheriser(FILE_DS, FILE_COUNTRY_STD, FILE_GRAPHER)
    elif step == "grapher-db":
        run_db_updater(FILE_GRAPHER)


if __name__ == "__main__":
    args = _parse_args()
    run_step(args.step)
