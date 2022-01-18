"""Update gapher database.

Update vaccination by manufacturer data.
"""


from cowidev.grapher.db.base import GrapherBaseUpdater


class GrapherVaxAgeUpdater(GrapherBaseUpdater):
    def __init__(self) -> None:
        super().__init__(
            dataset_name="COVID-19 - Vaccinations by age group",
            source_name="Official data collated by Our World in Data",
            zero_day="2021-01-01",
            slack_notifications=False,
        )
