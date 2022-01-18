"""Updates to grapher database.

These are executed in grapherupdate.sh, by calling `cowidev-grapher-db`.

Some grapher updates are run separately, by means of run_grapher_db step in library step.
"""
import traceback

from cowidev.grapher.db.procs.testing import GrapherTestUpdater
from cowidev.grapher.db.procs.variants import GrapherVariantsUpdater, GrapherSequencingUpdater
from cowidev.grapher.db.procs.vax_age import GrapherVaxAgeUpdater
from cowidev.grapher.db.procs.vax_manufacturer import GrapherVaxManufacturerUpdater
from cowidev.grapher.db.procs.vax import GrapherVaxUpdater
from cowidev.grapher.db.procs.vax_us import GrapherUSVaxUpdater
from cowidev.grapher.db.procs.yougov_composite import GrapherYougovCompUpdater
from cowidev.grapher.db.procs.yougov import GrapherYougovUpdater
from cowidev.grapher.db.utils.slack_client import send_error


updaters = [
    GrapherTestUpdater,
    GrapherVariantsUpdater,
    GrapherSequencingUpdater,
    GrapherVaxAgeUpdater,
    GrapherVaxManufacturerUpdater,
    GrapherVaxUpdater,
    GrapherUSVaxUpdater,
    GrapherYougovCompUpdater,
    GrapherYougovUpdater,
]
updaters = [u() for u in updaters]


def main():
    for updater in updaters:
        try:
            updater.run()
        except Exception as e:
            tb = traceback.format_exc()
            send_error(
                channel="corona-data-updates",
                title=f"Updating Grapher dataset: {updater.dataset_name}",
                trace=tb,
            )
