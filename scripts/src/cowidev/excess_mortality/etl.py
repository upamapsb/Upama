"""Get excess mortality dataset and publish it in public/data."""


from datetime import datetime, timedelta

import pandas as pd
from cowidev.utils.utils import export_timestamp


class XMortalityETL:
    def __init__(self) -> None:
        self.source_url = (
            "https://github.com/owid/owid-datasets/raw/master/datasets/"
            "Excess%20Mortality%20Data%20%E2%80%93%20OWID%20(2021)/"
            "Excess%20Mortality%20Data%20%E2%80%93%20OWID%20(2021).csv"
        )
        self.timestamp_filename = "owid-covid-data-last-updated-timestamp-xm.txt"

    def extract(self):
        return pd.read_csv(self.source_url)

    def pipeline(self, df: pd.DataFrame):
        # Rename columns
        df = df.rename(
            columns={
                "Entity": "location",
                "Year": "date",
                "Excess mortality P-scores, all ages": "p_scores_all_ages",
                "Excess mortality P-scores, ages 0–14": "p_scores_0_14",
                "Excess mortality P-scores, ages 15–64": "p_scores_15_64",
                "Excess mortality P-scores, ages 65–74": "p_scores_65_74",
                "Excess mortality P-scores, ages 75–84": "p_scores_75_84",
                "Excess mortality P-scores, ages 85+": "p_scores_85plus",
                "Deaths, 2020, all ages": "deaths_2020_all_ages",
                "Average deaths, 2015–2019, all ages": "average_deaths_2015_2019_all_ages",
            }
        )
        # Fix date
        df.loc[:, "date"] = [(datetime(2020, 1, 1) + timedelta(days=d)).strftime("%Y-%m-%d") for d in df.date]
        # Sort rows
        df = df.sort_values(["location", "date"])
        return df

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.pipe(self.pipeline)

    def load(self, df: pd.DataFrame, output_path: str) -> None:
        # Export data
        df.to_csv(output_path, index=False)
        export_timestamp(self.timestamp_filename)

    def run(self, output_path: str):
        df = self.extract()
        df = self.transform(df)
        self.load(df, output_path)


def run_etl(output_path: str):
    etl = XMortalityETL()
    etl.run(output_path)
