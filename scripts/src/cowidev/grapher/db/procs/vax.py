from cowidev.grapher.db.base import GrapherBaseUpdater


class GrapherVaxUpdater(GrapherBaseUpdater):
    def __init__(self) -> None:
        super().__init__(
            dataset_name="COVID-19 - Vaccinations",
            source_name=f"Official data collated by Our World in Data – Last updated {self.time_str} (London time)",
            zero_day="2020-01-21",
        )
