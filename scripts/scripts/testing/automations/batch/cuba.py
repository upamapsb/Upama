from datetime import datetime

import requests

import pandas as pd


class Cuba:
    def __init__(self):
        self.source_url = (
            "https://raw.githubusercontent.com/covid19cubadata/covid19cubadata.github.io/master/data/covid19-cuba.json"
        )
        self.source_url_ref = "https://covid19cubadata.github.io/#cuba"
        self.location = "Cuba"

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
                        "Date": datetime.strptime(elem["fecha"], "%Y/%m/%d").strftime("%Y-%m-%d"),
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

    def to_csv(self):
        output_path = f"automated_sheets/{self.location}.csv"
        df = self.read().pipe(self.pipeline)
        df.to_csv(output_path, index=False)


def main():
    Cuba().to_csv()


if __name__ == "__main__":
    main()
