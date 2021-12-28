import pandas as pd

from cowidev.utils import clean_date_series
from cowidev.testing import CountryTestBase
from cowidev.testing.utils import make_monotonic


class Austria(CountryTestBase):
    location: str = "Austria"
    units: str = "tests performed"
    source_url: str = "https://covid19-dashboard.ages.at/data/CovidFallzahlen.csv"
    source_url_ref: str = "https://www.data.gv.at/katalog/dataset/846448a5-a26e-4297-ac08-ad7040af20f1"
    source_name: str = "Federal Ministry for Social Affairs, Health, Care and Consumer Protection"

    def read(self):
        return pd.read_csv(self.source_url, sep=";", usecols=["Meldedat", "TestGesamt", "Bundesland"])

    def pipeline(self, df: pd.DataFrame):
        df = df[df.Bundesland == "Alle"]
        df = df.groupby("Meldedat", as_index=False)["TestGesamt"].sum()
        df = df.rename(
            columns={
                "Meldedat": "Date",
                "TestGesamt": "Cumulative total",
            }
        )

        df = df.assign(
            **{
                "Country": self.location,
                "Units": self.units,
                "Source URL": self.source_url_ref,
                "Source label": self.source_name,
                "Notes": pd.NA,
                "Date": clean_date_series(df.Date, "%d.%m.%Y"),
            }
        )

        df = df.sort_values("Cumulative total").groupby("Cumulative total", as_index=False).head(1).sort_values("Date")
        return df

    def export(self):
        df = self.read().pipe(self.pipeline).pipe(make_monotonic)
        self.export_datafile(df)


def main():
    Austria().export()


if __name__ == "__main__":
    main()
