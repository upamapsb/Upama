import pandas as pd

from cowidev.testing import CountryTestBase
from cowidev.utils.web import request_json


class Bangladesh(CountryTestBase):
    location: str = "Bangladesh"

    def main(self):

        data = request_json(
            "https://services3.arcgis.com/nIl76MjbPamkQiu8/arcgis/rest/services/date_wise_corona_with_positive_percentage/FeatureServer/0/query?f=json&where=1%3D1&returnGeometry=false&spatialRel=esriSpatialRelIntersects&outFields=*&orderByFields=date%20asc&outSR=102100&resultOffset=0&resultRecordCount=32000&resultType=standard&cacheHint=true"
        )
        data = data["features"]
        dates = [day["attributes"]["date"] for day in data]
        tested = [day["attributes"]["tested"] for day in data]

        df = pd.DataFrame(
            {
                "Date": pd.to_datetime(dates, unit="ms").date,
                "Daily change in cumulative total": tested,
            }
        ).dropna()
        df = df[df["Daily change in cumulative total"] != 0]

        df.loc[:, "Daily change in cumulative total"] = df["Daily change in cumulative total"].astype(int)
        df.loc[:, "Country"] = self.location
        df.loc[:, "Units"] = "tests performed"
        df.loc[:, "Source label"] = "Government of Bangladesh"
        df.loc[:, "Source URL"] = "https://covid19bd.idare.io/"
        df.loc[:, "Notes"] = pd.NA

        # Manual fix for error in data
        df.loc[
            (pd.to_datetime(df["Date"]) == pd.to_datetime("2020-03-16"))
            & (df["Daily change in cumulative total"] == 39),
            "Date",
        ] = "2020-03-17"

        self.export_datafile(df)


if __name__ == "__main__":
    Bangladesh().main()
