import os
import pandas as pd
from cowidev.utils.clean import clean_date_series

class Italy:
    location: str = "Italy"
    units: str = "tests performed"
    source_label: str = "Presidency of the Council of Ministers"
    source_url: str = "https://raw.githubusercontent.com/pcm-dpc/COVID-19/master/dati-andamento-nazionale/dpc-covid19-ita-andamento-nazionale.csv"
    source_url_ref: str = "https://github.com/pcm-dpc/COVID-19/tree/master/dati-andamento-nazionale/"
    notes: str = "Made available by the Department of Civil Protection on GitHub"
    rename_columns: dict = {"data": "Date", "tamponi": "Cumulative total"}

    def read(self) -> pd.DataFrame:
        df = pd.read_csv(self.source_url, usecols=["data", "tamponi"])
        df["data"] = df["data"].str.replace("T"," ")
        df["data"] = clean_date_series(df["data"], "%Y-%m-%d %H:%M:%S")
        return df

    def pipe_rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.rename(columns=self.rename_columns)

    def pipe_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.sort_values("Date")

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
    Italy().export()


if __name__ == "__main__":
    main()
