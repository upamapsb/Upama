from cowidev.grapher.db.base import GrapherBaseUpdater


class GrapherYougovUpdater(GrapherBaseUpdater):
    def __init__(self) -> None:
        super().__init__(
            dataset_name="YouGov-Imperial COVID-19 Behavior Tracker",
            source_name=(
                f"Imperial College London YouGov Covid 19 Behaviour Tracker Data Hub – Last updated {self.time_str} "
                f"(London time)"
            ),
            zero_day="2020-01-21",
        )
