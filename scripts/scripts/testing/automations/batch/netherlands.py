import os
import pandas as pd
from cowidev.utils.web import request_json
from cowidev.utils.utils import get_project_dir


class Netherlands:
    location: str = "Netherlands"
    units: str = "tests performed"
    source_label: str = "Dutch National Institute for Public Health and the Environment"
    source_url: str = "https://data.rivm.nl/covid-19/COVID-19_uitgevoerde_testen.json"
    source_url_ref: str = "https://data.rivm.nl/covid-19/"
    notes: str = pd.NA
    rename_columns: dict = {"Date_of_statistics": "Date", "Tested_with_result": "Daily change in cumulative total"}

    def read(self) -> pd.DataFrame:
        data = request_json(self.source_url)
        df = pd.DataFrame.from_records(data, columns=["Date_of_statistics", "Tested_with_result", "Security_region_name"])
        df = df.groupby("Date_of_statistics").sum().reset_index()
        return df

    def pipe_rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.rename(columns=self.rename_columns)

    def pipe_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(
            **{
                "Country": self.location,
                "Units": self.units,
                "Source label": self.source_label,
                "Source URL": self.source_url_ref,
                "Notes": self.notes,
            }
        )

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.pipe(self.pipe_rename_columns).pipe(self.pipe_metadata)

    def export(self):
        path = os.path.join(
            get_project_dir(), "scripts", "scripts", "testing", "automated_sheets", f"{self.location}.csv"
        )
        df = self.read().pipe(self.pipeline)
        df.to_csv(path, index=False)


def main():
    Netherlands().export()


if __name__ == "__main__":
    main()