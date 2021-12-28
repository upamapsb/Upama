import pandas as pd

from cowidev.testing import CountryTestBase


class Slovakia(CountryTestBase):
    location: str = "Slovakia"

    def export(self):

        df = pd.read_csv(
            "https://github.com/Institut-Zdravotnych-Analyz/covid19-data/raw/main/DailyStats/OpenData_Slovakia_Covid_DailyStats.csv",
            sep=";",
            usecols=[
                "Datum",
                "Dennych.PCR.testov",
                "AgTests",
                "Dennych.PCR.prirastkov",
                "AgPosit",
            ],
        )

        df = df.sort_values("Datum")
        df["Daily change in cumulative total"] = df["Dennych.PCR.testov"].fillna(0) + df["AgTests"].fillna(0)
        df["positive"] = df["Dennych.PCR.prirastkov"].fillna(0) + df["AgPosit"].fillna(0)
        df["Positive rate"] = (
            df.positive.rolling(7).mean() / df["Daily change in cumulative total"].rolling(7).mean()
        ).round(3)

        df = df[["Datum", "Daily change in cumulative total", "Positive rate"]].rename(columns={"Datum": "Date"})

        df.loc[:, "Source URL"] = "https://github.com/Institut-Zdravotnych-Analyz/covid19-data"
        df.loc[:, "Source label"] = "Ministry of Health"
        df.loc[:, "Country"] = "Slovakia"
        df.loc[:, "Units"] = "tests performed"
        df.loc[:, "Notes"] = pd.NA

        self.export_datafile(df)


def main():
    Slovakia().export()


if __name__ == "__main__":
    main()
