import os
import pandas as pd
from cowidev.utils.clean import clean_date_series


class Malta:
    location: str = "Malta"
    units: str = "tests performed"
    source_label: str = "COVID-19 Public Health Response Team (Ministry for Health)"
    source_url: str = "https://raw.githubusercontent.com/COVID19-Malta/COVID19-Data/master/COVID-19%20Malta%20-%20COVID%20Tests.csv"
    source_url_ref: str = "https://github.com/COVID19-Malta/COVID19-Data/"
    notes: str = pd.NA
    rename_columns: dict = {"Publication date": "Date", "Total NAA and rapid antigen tests": "Cumulative total"}

    def read(self) -> pd.DataFrame:
        df = pd.read_csv(self.source_url, usecols=["Publication date", "Total NAA and rapid antigen tests"])
        df["Publication date"] = clean_date_series(df["Publication date"], "%d/%m/%Y")
        return df

    def pipe_rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.rename(columns=self.rename_columns)

    def pipe_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.sort_values("Date")
        return df

    def pipe_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(
            **{
                "Country": self.location,
                "Source URL": self.source_url_ref,
                "Source label": self.source_label,
                "Notes": self.notes,
                "Units": self.units,
            }
        )

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.pipe(self.pipe_rename_columns).pipe(self.pipe_metrics).pipe(self.pipe_metadata)

    def export(self):
        df = self.read().pipe(self.pipeline)
        df.to_csv(os.path.join("automated_sheets", f"{self.location}.csv"), index=False)


def main():
    Malta().export()


if __name__ == "__main__":
    main()
