"""constructs a daily time series for Finland of the daily change in COVID-19 tests.
API documentation: https://thl.fi/fi/tilastot-ja-data/aineistot-ja-palvelut/avoin-data/varmistetut-koronatapaukset-suomessa-covid-19-
"""

import requests
import pandas as pd

from cowidev.testing import CountryTestBase


class Finland(CountryTestBase):
    location = "Finland"
    units = "tests performed"
    source_url = "https://services7.arcgis.com/nuPvVz1HGGfa0Eh7/arcgis/rest/services/korona_testimaara_paivittain/FeatureServer/0/query?f=json&where=date%3Etimestamp%20%272020-02-25%2022%3A59%3A59%27&returnGeometry=false&spatialRel=esriSpatialRelIntersects&outFields=OBJECTID%2Ctestimaara_kumulatiivinen%2Cdate&orderByFields=date%20asc&resultOffset=0&resultRecordCount=4000&resultType=standard&cacheHint=true"
    source_url_ref = "https://experience.arcgis.com/experience/d40b2aaf08be4b9c8ec38de30b714f26"
    source_label = "Finnish Department of Health and Welfare"

    def read(self):
        data = requests.get(self.source_url).json()["features"]
        dates = [d.get("attributes").get("date") for d in data]
        dates = pd.to_datetime(dates, unit="ms").date
        total_tests = [d.get("attributes").get("testimaara_kumulatiivinen") for d in data]
        # build dataframe
        df = pd.DataFrame({"Date": dates, "Cumulative total": total_tests})
        return df

    def pipeline(self, df: pd.DataFrame):
        return df.groupby("Cumulative total", as_index=False).min().pipe(self.pipe_metadata)

    def export(self):
        df = self.read().pipe(self.pipeline)
        self.export_datafile(df)


def main():
    Finland().export()


if __name__ == "__main__":
    main()
