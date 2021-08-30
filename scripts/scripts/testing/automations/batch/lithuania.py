import os

import pandas as pd


class Lithuania:
    location: str = "Lithuania"
    units: str = "tests performed"
    source_label: str = "Government of Lithuania"
    source_url: str = "https://opendata.arcgis.com/datasets/d49a63c934be4f65a93b6273785a8449_0.csv?where=municipality_code%20%3D%20%2700%27"
    source_url_ref: str = "https://open-data-ls-osp-sdg.hub.arcgis.com/datasets/d49a63c934be4f65a93b6273785a8449_0"
    notes: str = pd.NA
    rename_columns: dict = {
        "date": "Date",
        "dgn_pos_day": "daily_pos",
        "dgn_tot_day": "Daily change in cumulative total",
    }

    def read(self) -> pd.DataFrame:
        return pd.read_csv(self.source_url, usecols=["date", "dgn_pos_day", "dgn_tot_day"], parse_dates=["date"])

    def pipe_rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.rename(columns=self.rename_columns)

    def pipe_filter(self, df: pd.DataFrame) -> pd.DataFrame:
        return df[df["Daily change in cumulative total"] > 0]

    def pipe_date(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.assign(Date=df.Date.dt.strftime("%Y-%m-%d"))
        return df.sort_values("Date")

    def pipe_pr(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(
            **{
                "Positive rate": (
                    df["daily_pos"].rolling(7).sum() / df["Daily change in cumulative total"].rolling(7).sum()
                ).round(3)
            }
        )

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

    def pipe_out_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.drop(columns=["daily_pos"])

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.pipe(self.pipe_rename_columns)
            .pipe(self.pipe_filter)
            .pipe(self.pipe_date)
            .pipe(self.pipe_pr)
            .pipe(self.pipe_metadata)
            .pipe(self.pipe_out_columns)
        )

    def export(self):
        df = self.read().pipe(self.pipeline)
        df.to_csv(os.path.join("automated_sheets", f"{self.location}.csv"), index=False)


def main():
    Lithuania().export()


if __name__ == "__main__":
    main()
