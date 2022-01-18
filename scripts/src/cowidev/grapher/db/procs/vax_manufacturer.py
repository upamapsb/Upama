"""Update gapher database.

Update vaccination by manufacturer data.
"""


from cowidev.grapher.db.base import GrapherBaseUpdater


class GrapherVaxManufacturerUpdater(GrapherBaseUpdater):
    def __init__(self) -> None:
        super().__init__(
            dataset_name="COVID-19 - Vaccinations by manufacturer",
            source_name="Official data collated by Our World in Data",
            zero_day="2021-01-01",
        )
