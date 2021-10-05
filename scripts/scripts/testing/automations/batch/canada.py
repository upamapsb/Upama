import os

import pandas as pd


class Canada:
    def __init__(self):
        self.source_url = "https://health-infobase.canada.ca/src/data/covidLive/covid19-download.csv"
        self.location = "Canada"

    def read(self):
        df = pd.read_csv(self.source_url, usecols=["prname", "date", "numtested", "numtests"])
        df = df[df.prname == "Canada"]
        return df

    def pipeline_base(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.assign(
                **{
                    "Source label": "Government of Canada",
                    "Source URL": self.source_url,
                    "Notes": pd.NA,
                }
            )
            .rename(
                columns={
                    "prname": "Country",
                    "date": "Date",
                }
            )
            .sort_values("Date")
        )

    def pipeline_metric(self, df: pd.DataFrame, units: str, metric_field: str) -> pd.DataFrame:
        if metric_field == "numtested":
            metric_drop = "numtests"
        elif metric_field == "numtests":
            metric_drop = "numtested"
        else:
            raise ValueError("Invalid metric")
        df = (
            df.copy()
            .assign(
                **{
                    "Units": units,
                }
            )
            .rename(
                columns={
                    metric_field: "Cumulative total",
                }
            )
            .drop(columns=[metric_drop])
            .sort_values("Date")
            .drop_duplicates(subset=["Cumulative total"], keep="last")
        )
        df = df[~df["Cumulative total"].isna()]
        return df

    def to_csv(self):
        df = self.read().pipe(self.pipeline_base)
        # People
        output_path = os.path.join("automated_sheets", f"{self.location} - people tested.csv")
        df.pipe(self.pipeline_metric, "people tested", "numtested").to_csv(output_path, index=False)
        # Tests
        output_path = os.path.join("automated_sheets", f"{self.location} - tests performed.csv")
        df.pipe(self.pipeline_metric, "tests performed", "numtests").to_csv(output_path, index=False)


def main():
    Canada().to_csv()


if __name__ == "__main__":
    main()
