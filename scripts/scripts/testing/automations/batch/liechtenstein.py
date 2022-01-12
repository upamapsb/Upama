import pandas as pd

from cowidev.testing import CountryTestBase
from cowidev.utils.web import request_json


class Liechtenstein(CountryTestBase):
    location = "Liechtenstein"
    units = "tests performed"
    source_url = (
        "https://www.covid19.admin.ch/api/data/20220112-m4gbccen/sources/COVID19Test_geoRegion_PCR_Antigen.json"
    )
    source_url_ref = "https://opendata.swiss/en/dataset/covid-19-schweiz"
    source_label = "Federal Office of Public Health"
    rename_columns = {"datum": "Date", "entries": "Daily change in cumulative total"}

    def read(self) -> pd.DataFrame:
        json_dict = request_json(self.source_url)
        df = pd.DataFrame(json_dict)[["datum", "entries", "geoRegion"]]
        df = df[df.geoRegion == "FL"]
        return df

    def pipe_filter_rows(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.dropna(subset=["entries"])

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.pipe(self.pipe_filter_rows).pipe(self.pipe_rename_columns).pipe(self.pipe_metadata)

    def export(self):
        df = self.read().pipe(self.pipeline)
        self.export_datafile(df)


def main():
    Liechtenstein().export()


if __name__ == "__main__":
    main()
