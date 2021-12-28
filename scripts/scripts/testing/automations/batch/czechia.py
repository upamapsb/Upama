import pandas as pd

from cowidev.testing import CountryTestBase


class Czechia(CountryTestBase):
    location: str = "Czechia"
    source_url: str = "https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19/testy-pcr-antigenni.csv"

    def read(self):
        return pd.read_csv(
            self.source_url,
            usecols=["datum", "pocet_PCR_testy", "pocet_AG_testy", "incidence_pozitivni"],
        )

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        # Rename
        df = df.rename(
            columns={
                "datum": "Date",
                "pocet_PCR_testy": "pcr",
                "pocet_AG_testy": "antigen",
                "incidence_pozitivni": "positive",
            }
        )
        # New columns
        df = df.assign(
            **{
                "Daily change in cumulative total": df.pcr + df.antigen,
                "Country": self.location,
                "Units": "tests performed",
                "Source URL": "https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19",
                "Source label": "Ministry of Health",
                "Notes": pd.NA,
                "Cumulative total": pd.NA,
            }
        )
        df = df.assign(
            **{
                "Positive rate": (
                    (df["positive"].rolling(7).sum() / df["Daily change in cumulative total"].rolling(7).sum()).round(
                        3
                    )
                )
            }
        )
        # Order
        df = df.sort_values("Date")
        # Output
        df = df[
            [
                "Date",
                "Daily change in cumulative total",
                "Positive rate",
                "Cumulative total",
                "Country",
                "Units",
                "Source URL",
                "Source label",
                "Notes",
            ]
        ]
        return df

    def export(self):
        df = self.read().pipe(self.pipeline)
        self.export_datafile(df)


def main():
    Czechia().export()


if __name__ == "__main__":
    main()
