import pandas as pd

from cowidev.testing import CountryTestBase
from cowidev.testing.utils import make_monotonic
from cowidev.utils import clean_date_series


class Austria(CountryTestBase):
    location: str = "Austria"
    units: str = "tests performed"
    source_url: str = "https://covid19-dashboard.ages.at/data/CovidFallzahlen.csv"
    source_url_ref: str = "https://www.data.gv.at/katalog/dataset/846448a5-a26e-4297-ac08-ad7040af20f1"
    source_label: str = "Federal Ministry for Social Affairs, Health, Care and Consumer Protection"
    rename_columns: str = {
        "Meldedat": "Date",
        "TestGesamt": "Cumulative total",
    }

    def read(self) -> pd.DataFrame:
        df = pd.read_csv(self.source_url, sep=";", usecols=["Meldedat", "TestGesamt", "Bundesland"])
        df = df[df.Bundesland == "Alle"]
        df = df.groupby("Meldedat", as_index=False)["TestGesamt"].sum()
        return df

    def pipe_date(self, df: pd.DataFrame):
        return df.assign(Date=clean_date_series(df["Date"], "%d.%m.%Y"))

    def pipe_filter(self, df: pd.DataFrame):
        df = df.sort_values("Cumulative total").groupby("Cumulative total", as_index=False).head(1).sort_values("Date")
        return df

    def pipeline(self, df: pd.DataFrame):
        return (
            df.pipe(self.pipe_rename_columns)
            .pipe(self.pipe_date)
            .pipe(self.pipe_metadata)
            .pipe(self.pipe_filter)
            .pipe(make_monotonic)
        )

    def export(self):
        df = self.read().pipe(self.pipeline)
        self.export_datafile(df)


def main():
    Austria().export()


if __name__ == "__main__":
    main()
