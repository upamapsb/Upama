import pandas as pd


class UnitedStates:
    location: str = "United States"
    source_url: str = "https://healthdata.gov/api/views/j8mb-icvb/rows.csv"
    source_url_ref: str = "https://healthdata.gov/dataset/COVID-19-Diagnostic-Laboratory-Testing-PCR-Testing/j8mb-icvb"
    source_label: str = "Department of Health & Human Services"

    def read(self):
        df = pd.read_csv(self.source_url, usecols=["date", "new_results_reported"], parse_dates=["date"])
        return df

    def pipe_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(
            **{
                "Country": self.location,
                "Units": "tests performed",
                "Source label": self.source_label,
                "Source URL": self.source_url_ref,
                "Notes": pd.NA,
            }
        )

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.pipe(pipe_rename_columns).pipe(pipe_metrics).pipe(pipe_date).pipe(self.pipe_metadata)
        return df

    def export(self):
        output_path = f"automated_sheets/{self.location}.csv"
        df = self.read().pipe(self.pipeline)
        df.to_csv(output_path, index=False)


def pipe_rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(columns={"date": "Date"})


def pipe_metrics(df: pd.DataFrame) -> pd.DataFrame:
    return df.groupby("Date", as_index=False).agg(
        **{"Daily change in cumulative total": ("new_results_reported", sum)}
    )


def pipe_date(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(Date=df.Date.dt.strftime("%Y-%m-%d"))


def main():
    UnitedStates().export()


if __name__ == "__main__":
    main()
