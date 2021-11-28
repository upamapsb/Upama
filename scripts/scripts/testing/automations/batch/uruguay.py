import json
import os
from urllib.request import Request, urlopen

import pandas as pd


class Uruguay:
    location: str = "Uruguay"
    units: str = "people tested"
    source_label: str = "Ministry of Public Health"
    base_url: str = "https://estadisticas.msp-uy.com/data.json"
    notes = ""
    testing_type: str = "PCR only"
    rename_columns: dict = {"date": "Date", "total": "Cumulative total"}

    @property
    def source_url(self):
        return self.base_url.format("raw")

    @property
    def source_url_ref(self):
        return self.base_url.format("blob")

    def read(self) -> pd.DataFrame:
        body_json = urlopen(
            Request(self.source_url, headers={'User-Agent': 'Mozilla/5.0'})
        ).read().decode('UTF-8')
        json_dict = json.loads(body_json)
        df = pd.DataFrame.from_dict(json_dict["tests"]["historical"])
        df['date'] = pd.to_datetime(df['date'])
        return df

    def pipe_rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.rename(columns=self.rename_columns)

    @staticmethod
    def pipe_date(df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(Date=df.Date.dt.strftime("%Y-%m-%d"))

    def pipe_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(
            **{
                "Country": self.location,
                "Units": self.units,
                "Source label": self.source_label,
                "Source URL": self.source_url_ref,
                "Notes": self.notes,
            }
        )

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.pipe(self.pipe_rename_columns)
            .pipe(self.pipe_date)
            .pipe(self.pipe_metadata)
        )

    def export(self):
        df = self.read().pipe(self.pipeline)
        df.to_csv(os.path.join("automated_sheets", f"{self.location}.csv"), index=False)


def main():
    Uruguay().export()


if __name__ == "__main__":
    main()

