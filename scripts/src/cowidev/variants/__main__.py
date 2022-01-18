import os

from cowidev.utils.utils import get_project_dir
from cowidev.variants.etl import run_etl
from cowidev.variants.grapher import run_grapheriser, run_explorerizer, run_db_updater
from cowidev.variants._parser import _parse_args


FILE_DS = "s3://covid-19/internal/variants/covid-variants.csv"
FILE_SEQ_DS = "s3://covid-19/internal/variants/covid-sequencing.csv"
# FILE_DS = "covid-variants.csv"
# FILE_SEQ_DS = "covid-sequencing.csv"
# FILE_SEQ_DS = os.path.join(get_project_dir(), "public", "data", "variants", "covid-sequencing.csv")
FILE_GRAPHER = os.path.join(get_project_dir(), "scripts", "grapher", "COVID-19 - Variants.csv")
FILE_SEQ_GRAPHER = os.path.join(get_project_dir(), "scripts", "grapher", "COVID-19 - Sequencing.csv")
FILE_EXPLORER = os.path.join(get_project_dir(), "public", "data", "internal", "megafile--variants.json")


def run_step(step: str):
    if step == "etl":
        run_etl(FILE_DS, FILE_SEQ_DS)
    elif step == "grapher-file":
        # Filter by num_seq
        run_grapheriser(FILE_DS, FILE_GRAPHER, FILE_SEQ_DS, FILE_SEQ_GRAPHER)
    elif step == "explorer-file":
        # Filter by num_seq
        run_explorerizer(FILE_DS, FILE_EXPLORER)
    elif step == "grapher-db":
        run_db_updater(FILE_GRAPHER)


if __name__ == "__main__":
    args = _parse_args()
    run_step(args.step)
