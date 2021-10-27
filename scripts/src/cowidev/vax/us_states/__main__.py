import os

from cowidev.utils.utils import get_project_dir

from cowidev.vax.us_states.etl import run_etl
from cowidev.vax.us_states.grapher import run_grapheriser
from cowidev.vax.us_states._parser import _parse_args


FILE_DS = os.path.join(get_project_dir(), "public", "data", "vaccinations", "us_state_vaccinations.csv")
FILE_GRAPHER = os.path.join(get_project_dir(), "scripts", "grapher", "COVID-19 - United States vaccinations.csv")


def run_step(step: str):
    if step == "etl":
        run_etl(FILE_DS)
    elif step == "grapher-file":
        run_grapheriser(FILE_DS, FILE_GRAPHER)
    # elif step == "explorer-file":
    #     run_explorerizer(FILE_DS, FILE_EXPLORER)
    # elif step == "grapher-db":
    #     run_db_updater(FILE_GRAPHER)


if __name__ == "__main__":
    args = _parse_args()
    run_step(args.step)
