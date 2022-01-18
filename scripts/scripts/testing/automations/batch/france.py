import pandas as pd

from cowidev.testing import CountryTestBase


class France(CountryTestBase):
    location = "France"
    units = "people tested"
    source_url_ref = (
        "https://www.data.gouv.fr/fr/datasets/donnees-relatives-aux-resultats-des-tests-virologiques-covid-19/"
    )
    source_label = "National Public Health Agency"

    def read(self):
        df_ppl = self._read_ppl_tested()
        df_pr = self._read_pr()
        df = pd.merge(df_ppl, df_pr, on="Date", how="outer").sort_values("Date")
        return df

    def _read_ppl_tested(self):
        url = "https://www.data.gouv.fr/fr/datasets/r/dd0de5d9-b5a5-4503-930a-7b08dc0adc7c"
        df = pd.read_csv(url, sep=";", usecols=["jour", "cl_age90", "T"])
        df = (
            df[df.cl_age90 == 0]
            .rename(columns={"jour": "Date", "T": "Daily change in cumulative total"})
            .drop(columns=["cl_age90"])
        )
        return df

    def _read_pr(self):
        df = pd.read_csv(
            "https://www.data.gouv.fr/fr/datasets/r/381a9472-ce83-407d-9a64-1b8c23af83df",
            usecols=["extract_date", "tx_pos"],
        )
        df.loc[:, "tx_pos"] = df["tx_pos"].div(100).round(3)
        df = df.rename(columns={"extract_date": "Date", "tx_pos": "Positive rate"})
        return df

    def pipeline(self, df: pd.DataFrame):
        df = df.dropna(subset=["Daily change in cumulative total", "Positive rate"], how="all")
        df = df.pipe(self.pipe_metadata)
        return df

    def export(self):
        df = self.read().pipe(self.pipeline)
        self.export_datafile(df)


def main():
    France().export()


if __name__ == "__main__":
    main()
