from cowidev.grapher.db.base import GrapherBaseUpdater


class GrapherTestUpdater(GrapherBaseUpdater):
    def __init__(self) -> None:
        super().__init__(
            dataset_name="COVID testing time series data",
            source_name=f"Official data collated by Our World in Data – Last updated {self.time_str} (London time)",
            zero_day="2020-01-21",
        )
