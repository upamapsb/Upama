import os

import pandas as pd


class Israel:
    def __init__(self):
        self.location = "Israel"
        self.source_url = "https://datadashboardapi.health.gov.il/api/queries/testResultsPerDate"

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
        output_path = os.path.join(f"automated_sheets", f"{self.location}.csv")
        df = self.read().pipe(self.pipeline)
        df.to_csv(output_path, index=False)


def main():
    Israel().to_csv()


if __name__ == "__main__":
    main()
