import pandas as pd
from uk_covid19 import Cov19API

from cowidev.testing import CountryTestBase


class UnitedKingdom(CountryTestBase):
    location = "United Kingdom"
    source_url_ref = "https://coronavirus.data.gov.uk/details/testing"
    source_label = "Public Health England"
    units = "tests performed"
    nation_dates_min = {
        "England": "2020-07-01",
        "Northern Ireland": "2020-06-25",
        "Scotland": "2020-06-14",
        "Wales": "2020-07-13",
    }

    def _read_nation(self, nation, date_min):
        filters = ["areaType=Nation", f"areaName={nation}"]
        structure = {
            "Date": "date",
            "Country": "areaName",
            "cumPillarOne": "cumPillarOneTestsByPublishDate",
            "newPillarTwo": "newPillarTwoTestsByPublishDate",
        }
        api = Cov19API(filters=filters, structure=structure)
        df = api.get_dataframe()
        df = df.assign(**{
            "cumPillarTwo": (
                df[pd.to_datetime(df["Date"]) > date_min]["newPillarTwo"][::-1].cumsum().fillna(method="ffill")
            ),
            "Cumulative total": df["cumPillarOne"] + df["cumPillarTwo"].fillna(0)
        })
        
        df["
        return df

    def read(self):
        countries = [self._read_nation(country, date) for country, date in self.nation_dates_min.items()]
        df = pd.concat(countries).sort_values("Date")
        return df

    def pipe_notes(self, df: pd.DataFrame):
        df[
            "Notes"
        ] = "PillarOne: England, N. Ireland, Scotland, Wales; PillarTwo: England, N. Ireland, Scotland, Wales"

        df.loc[df.Date < "2020-06-15", "Notes"] = "PillarOne: England, N. Ireland, Scotland, Wales; PillarTwo: None"
        df.loc[
            df.Date < "2020-06-26", "Notes"
        ] = "PillarOne: England, N. Ireland, Scotland, Wales; PillarTwo: Scotland"
        df.loc[
            df.Date < "2020-07-02", "Notes"
        ] = "PillarOne: England, N. Ireland, Scotland, Wales; PillarTwo: N. Ireland, Scotland"
        df.loc[
            df.Date < "2020-07-14", "Notes"
        ] = "PillarOne: England, N. Ireland, Scotland, Wales; PillarTwo: England, N. Ireland, Scotland"
        return df

    def pipeline(self, df: pd.DataFrame):
        df = df.groupby("Date", as_index=False).agg({"Cumulative total": "sum"})
        df = df.pipe(self.pipe_notes).pipe(self.pipe_metadata)
        return df

    def export(self):
        df = self.read().pipe(self.pipeline)
        self.export_datafile(df)


def main():
    UnitedKingdom().export()


if __name__ == "__main__":
    main()
