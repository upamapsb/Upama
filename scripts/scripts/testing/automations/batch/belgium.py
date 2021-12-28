import pandas as pd

from cowidev.testing import CountryTestBase


class Belgium(CountryTestBase):
    location: str = "Belgium"

    def read(self, source_url):
        # Read
        return pd.read_csv(source_url, usecols=["DATE", "TESTS_ALL", "TESTS_ALL_POS"])

    def pipeline(self, df: pd.DataFrame, source_url: str, location: str) -> pd.DataFrame:
        df = df.groupby("DATE", as_index=False).sum()
        # Positive rate
        df = df.assign(
            **{"Positive rate": (df.TESTS_ALL_POS.rolling(7).sum() / df.TESTS_ALL.rolling(7).sum()).round(3)}
        )
        # Rename columns
        df = df.rename(
            columns={
                "DATE": "Date",
                "TESTS_ALL": "Daily change in cumulative total",
                "PR": "Positive rate",
            }
        )
        # Add columns
        df = df.assign(
            **{
                "Country": self.location,
                "Units": "tests performed",
                "Source URL": source_url,
                "Source label": "Sciensano (Belgian institute for health)",
                "Notes": pd.NA,
            }
        )
        # Order
        df = df.sort_values("Date")
        # Output
        df = df[
            [
                "Date",
                "Daily change in cumulative total",
                "Positive rate",
                "Country",
                "Units",
                "Source URL",
                "Source label",
                "Notes",
            ]
        ]
        return df

    def main(self):
        source_url = "https://epistat.sciensano.be/Data/COVID19BE_tests.csv"
        df = self.read(source_url).pipe(self.pipeline, source_url, self.location)
        self.export_datafile(df)


if __name__ == "__main__":
    Belgium().main()
