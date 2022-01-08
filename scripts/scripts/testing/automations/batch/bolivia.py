import pandas as pd
from cowidev.utils.clean import clean_date_series

from cowidev.testing import CountryTestBase


class Bolivia(CountryTestBase):
    location = "Bolivia"
    units = "tests performed"
    source_label = "Bolivia Ministry of Health"
    source_url = "https://raw.githubusercontent.com/dquintani/covid/main/pruebas_acum.csv"
    source_url_ref = "https://www.boligrafica.com/"
    notes = "Made available by BoliGráfica on GitHub"
    rename_columns = {"Unnamed: 0": "Date", "Bolivia": "Cumulative total"}

    def read(self) -> pd.DataFrame:
        df = pd.read_csv(self.source_url, usecols=["Unnamed: 0", "Bolivia"]).dropna()
        df["Bolivia"] = df["Bolivia"].astype(int)
        df["Unnamed: 0"] = clean_date_series(df["Unnamed: 0"])

        return df

    def pipe_filter_rows(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.drop_duplicates(subset="Cumulative total").drop_duplicates(subset="Date")

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.pipe(self.pipe_rename_columns).pipe(self.pipe_filter_rows).pipe(self.pipe_metadata)

    def export(self):
        df = self.read().pipe(self.pipeline)
        self.export_datafile(df)


def main():
    Bolivia().export()


if __name__ == "__main__":
    main()
