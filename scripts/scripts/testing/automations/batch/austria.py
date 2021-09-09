from datetime import datetime

import pandas as pd

from utils import make_monotonic


class Austria:
    def __init__(self):
        self.source_url = "https://covid19-dashboard.ages.at/data/CovidFallzahlen.csv"
        self.source_url_ref = "https://www.data.gv.at/katalog/dataset/846448a5-a26e-4297-ac08-ad7040af20f1"
        self.location = "Austria"

    def read(self):
        return pd.read_csv(self.source_url, sep=";", usecols=["Meldedat", "TestGesamt", "Bundesland"])

    def pipeline(self, df: pd.DataFrame):
        df = df[df.Bundesland == "Alle"]
        df = df.groupby("Meldedat", as_index=False)["TestGesamt"].sum()
        df = df.rename(
            columns={
                "Meldedat": "Date",
                "TestGesamt": "Cumulative total",
            }
        )

        df = df.assign(
            **{
                "Country": self.location,
                "Units": "tests performed",
                "Source URL": self.source_url_ref,
                "Source label": "Federal Ministry for Social Affairs, Health, Care and Consumer Protection",
                "Notes": pd.NA,
                "Date": df.Date.apply(lambda x: datetime.strptime(x, "%d.%m.%Y").strftime("%Y-%m-%d")),
            }
        )

        df = df.sort_values("Cumulative total").groupby("Cumulative total", as_index=False).head(1).sort_values("Date")
        return df

    def to_csv(self):
        output_path = f"automated_sheets/{self.location}.csv"
        df = self.read().pipe(self.pipeline).pipe(make_monotonic)
        df.to_csv(output_path, index=False)


def main():
    Austria().to_csv()


if __name__ == "__main__":
    main()
