import os
from cowidev.grapher.db.base import GrapherBaseUpdater
from cowidev.utils import paths


class GrapherVariantsUpdater(GrapherBaseUpdater):
    def __init__(self) -> None:
        super().__init__(
            dataset_name="COVID-19 - Variants",
            source_name=f"CoVariants.org and GISAID – Last updated {self.time_str} (London time)",
            zero_day="2020-01-21",
            unit="%",
            unit_short="%",
        )


class GrapherSequencingUpdater(GrapherBaseUpdater):
    def __init__(self) -> None:
        super().__init__(
            dataset_name="COVID-19 - Sequencing",
            input_csv_path=os.path.join(paths.DATA.VARIANTS, "covid-sequencing.csv"),
            source_name=f"CoVariants.org and GISAID – Last updated {self.time_str} (London time)",
            zero_day="2020-01-21",
        )
