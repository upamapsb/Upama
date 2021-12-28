import requests

import pandas as pd

from cowidev.testing import CountryTestBase
from cowidev.utils import clean_date
from cowidev.utils.web import request_json


class Cuba(CountryTestBase):
    source_url: str = (
        "https://raw.githubusercontent.com/covid19cubadata/covid19cubadata.github.io/master/data/covid19-cuba.json"
    )
    source_url_ref: str = "https://covid19cubadata.github.io/#cuba"
    location: str = "Cuba"

    def read(self):
        data = requests.get(self.source_url).json()
        data = data["casos"]["dias"]
        data = list(data.values())
        df = self._parse_data(data)
        return df

    def _parse_data(self, data):
        records = []
        for elem in data:
            if "tests_total" in elem:
                records.append(
                    {
                        "Date": clean_date(elem["fecha"], "%Y/%m/%d"),
                        "Cumulative total": elem["tests_total"],
                    }
                )
        return pd.DataFrame(records)

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.assign(
            **{
                "Country": self.location,
                "Source label": "Ministry of Public Health",
                "Source URL": self.source_url_ref,
                "Notes": "Made available on GitHub by covid19cubadata",
                "Units": "tests performed",
            }
        )
        return df

    def export(self):
        df = self.read().pipe(self.pipeline)
        self.export_datafile(df)


def main():
    Cuba().export()


if __name__ == "__main__":
    main()
