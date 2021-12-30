import json
from cowidev.utils.web import get_soup
from cowidev.testing.utils.incremental import increment


class Mongolia:
    location = "Mongolia"
    units = "samples tested"
    source_label = "Ministry of Health"
    source_url = "https://e-mongolia.mn/shared-api/api/covid-stat/daily"
    source_url_ref = "https://www1.e-mongolia.mn/covid-19"
    notes = ""

    def _parse_data(self):
        data = json.loads(get_soup(self.source_url).text)
        return {"count": data["data"]["testedPcrTotal"], "date": data["data"]["lastDataDate"]}

    def export(self):
        data = self._parse_data()
        increment(
            count=data["count"],
            sheet_name=self.location,
            country=self.location,
            units=self.units,
            date=data["date"],
            source_url=self.source_url,
            source_label=self.source_label,
        )


def main():
    Mongolia().export()


if __name__ == "__main__":
    main()
