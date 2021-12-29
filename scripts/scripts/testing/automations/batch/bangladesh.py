import pandas as pd

from cowidev.testing import CountryTestBase
from cowidev.utils import clean_date_series
from cowidev.utils.web import request_json


class Bangladesh(CountryTestBase):
    location: str = "Bangladesh"
    units: str = "tests performed"
    source_label: str = "Government of Bangladesh"
    source_url_ref: str = "https://covid19bd.idare.io/"
    source_url: str = "https://services3.arcgis.com/nIl76MjbPamkQiu8/arcgis/rest/services/date_wise_corona_with_positive_percentage/FeatureServer/0/query?f=json&where=1%3D1&returnGeometry=false&spatialRel=esriSpatialRelIntersects&outFields=*&orderByFields=date%20asc&outSR=102100&resultOffset=0&resultRecordCount=32000&resultType=standard&cacheHint=true"

    def read(self):
        data = request_json(self.source_url)
        data = data["features"]
        dates = [day["attributes"]["date"] for day in data]
        tested = [day["attributes"]["tested"] for day in data]
        df = pd.DataFrame(
            {
                "Date": clean_date_series(dates, unit="ms"),
                "Daily change in cumulative total": tested,
            }
        ).dropna()
        return df

    def pipeline(self, df: pd.DataFrame):
        df = df[df["Daily change in cumulative total"] != 0]

        df.loc[:, "Daily change in cumulative total"] = df["Daily change in cumulative total"].astype(int)
        df = df.pipe(self.pipe_metadata)

        # Manual fix for error in data
        df.loc[
            (df["Date"] == "2020-03-16") & (df["Daily change in cumulative total"] == 39),
            "Date",
        ] = "2020-03-17"
        return df

    def export(self):
        df = self.read().pipe(self.pipeline)
        self.export_datafile(df)


def main():
    Bangladesh().export()


if __name__ == "__main__":
    main()
