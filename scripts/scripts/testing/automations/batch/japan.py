import os

import pandas as pd


class Japan:
    location: str = "Japan"
    units: str = "people tested"
    source_label: str = "Ministry of Health, Labour and Welfare"
    source_url: str = "https://www.mhlw.go.jp/content/pcr_tested_daily.csv"
    notes: str = pd.NA
    rename_columns: dict = {"日付": "Date", "PCR 検査実施件数(単日)": "Daily change in cumulative total"}

    def read(self) -> pd.DataFrame:
        return pd.read_csv(self.source_url, usecols=self.rename_columns.keys(), parse_dates=["日付"])

    def pipe_rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.rename(columns=self.rename_columns)

    def pipe_date(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(Date=df.Date.dt.strftime("%Y-%m-%d"))

    def pipe_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(
            **{
                "Country": self.location,
                "Units": self.units,
                "Source URL": self.source_url,
                "Source label": self.source_label,
                "Notes": self.notes,
            }
        )

    def pipe_filter_rows(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.dropna(subset=["Daily change in cumulative total"])

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.pipe(self.pipe_rename_columns).pipe(self.pipe_date).pipe(self.pipe_metadata).pipe(self.pipe_filter_rows)
        )

    def export(self):
        df = self.read().pipe(self.pipeline)
        df.to_csv(os.path.join("automated_sheets", f"{self.location}.csv"), index=False)


def main():
    Japan().export()


if __name__ == "__main__":
    main()
