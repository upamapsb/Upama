import os

from cowidev.utils.utils import get_project_dir
from cowidev.hosp.etl import run_etl
from cowidev.hosp.grapher import run_grapheriser, run_db_updater
from cowidev.hosp._parser import _parse_args
from cowidev.utils import paths


FILE_DS = os.path.join(paths.DATA.HOSPITALIZATIONS, "covid-hospitalizations.csv")
FILE_GRAPHER = os.path.join(paths.SCRIPTS.GRAPHER, "COVID-2019 - Hospital & ICU.csv")


def run_step(step: str):
    if step == "etl":
        run_etl(FILE_DS)
    elif step == "grapher-file":
        run_grapheriser(FILE_DS, FILE_GRAPHER)
    elif step == "grapher-db":
        run_db_updater(FILE_GRAPHER)


if __name__ == "__main__":
    args = _parse_args()
    run_step(args.step)
