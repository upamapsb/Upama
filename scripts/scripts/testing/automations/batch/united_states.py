import pandas as pd

from cowidev.testing import CountryTestBase
from cowidev.utils import clean_date_series


class UnitedStates(CountryTestBase):
    location = "United States"
    source_url = "https://healthdata.gov/api/views/j8mb-icvb/rows.csv"
    source_url_ref = "https://healthdata.gov/dataset/COVID-19-Diagnostic-Laboratory-Testing-PCR-Testing/j8mb-icvb"
    source_label = "Department of Health & Human Services"
    units = "tests performed"
    rename_columns = {"date": "Date"}

    def read(self):
        df = pd.read_csv(self.source_url, usecols=["date", "new_results_reported"], parse_dates=["date"])
        return df

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.pipe(self.pipe_rename_columns).pipe(pipe_metrics).pipe(pipe_date).pipe(self.pipe_metadata)
        return df

    def export(self):
        df = self.read().pipe(self.pipeline)
        self.export_datafile(df)


def pipe_metrics(df: pd.DataFrame) -> pd.DataFrame:
    return df.groupby("Date", as_index=False).agg(
        **{"Daily change in cumulative total": ("new_results_reported", sum)}
    )


def pipe_date(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(Date=clean_date_series(df.Date, "%Y-%m-%d"))


def main():
    UnitedStates().export()


if __name__ == "__main__":
    main()
