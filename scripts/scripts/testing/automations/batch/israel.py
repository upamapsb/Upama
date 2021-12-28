import pandas as pd


from cowidev.testing import CountryTestBase


class Israel(CountryTestBase):
    location: str = "Israel"
    source_url: str = "https://datadashboardapi.health.gov.il/api/queries/testResultsPerDate"

    def read(self):
        df = pd.read_json(self.source_url)[["date", "amount"]]
        return df

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.rename(
            columns={
                "date": "Date",
                "amount": "Daily change in cumulative total",
            }
        )
        df = df.assign(
            **{
                "Date": df.Date.dt.strftime("%Y-%m-%d"),
                "Country": self.location,
                "Units": "tests performed",
                "Source label": "Israel Ministry of Health",
                "Source URL": self.source_url,
                "Notes": pd.NA,
            }
        )
        return df

    def to_csv(self):
        df = self.read().pipe(self.pipeline)
        self.export_datafile(df)


def main():
    Israel().to_csv()


if __name__ == "__main__":
    main()
