import requests
import pandas as pd

from cowidev.utils.web import get_soup
from cowidev.utils.clean import extract_clean_date
from cowidev.testing.utils.incremental import increment


class NewZealand:
    location: str = "New Zealand"
    units: str = "tests performed"
    source_label: str = "Ministry of Health"
    source_url: str = "https://www.health.govt.nz/our-work/diseases-and-conditions/covid-19-novel-coronavirus/covid-19-data-and-statistics/covid-19-testing-data"
    notes: str = ""

    def _parse_data(self):
        soup = get_soup(self.source_url)
        quests = requests.get(self.source_url)
        table = pd.read_html(quests.text, index_col=0)[1]

        date_raw = soup.select_one(".pane-content .georgia-italic").text
        return {
            "count": int(table.loc["All COVID-19 viral tests administered*"]),
            "date": extract_clean_date(date_raw, regex="(\\d+ \\w+ 202\\d)", date_format="%d %B %Y"),
        }

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
    NewZealand().export()


if __name__ == "__main__":
    main()
