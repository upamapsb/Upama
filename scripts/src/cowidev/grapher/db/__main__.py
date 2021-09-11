import traceback

from cowidev.grapher.db.procs.testing import GrapherTestUpdater
from cowidev.grapher.db.procs.variants import GrapherVariantsUpdater
from cowidev.grapher.db.procs.vax_age import GrapherVaxAgeUpdater
from cowidev.grapher.db.procs.vax_manufacturer import GrapherVaxManufacturerUpdater
from cowidev.grapher.db.procs.vax import GrapherVaxUpdater
from cowidev.grapher.db.procs.yougov_composite import GrapherYougovCompUpdater
from cowidev.grapher.db.procs.yougov import GrapherYougovUpdater
from cowidev.grapher.db.utils.slack_client import send_error


updaters = [
    GrapherTestUpdater,
    GrapherVariantsUpdater,
    GrapherVaxAgeUpdater,
    GrapherVaxManufacturerUpdater,
    GrapherVaxUpdater,
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
