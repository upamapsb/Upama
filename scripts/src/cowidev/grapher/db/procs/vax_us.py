from cowidev.grapher.db.base import GrapherBaseUpdater


class GrapherUSVaxUpdater(GrapherBaseUpdater):
    def __init__(self) -> None:
        super().__init__(
            dataset_name="COVID-19 - United States vaccinations",
            source_name=f"Centers for Disease Control and Prevention – Last updated {self.time_str} (Eastern Time)",
            zero_day="2020-01-01",
        )
