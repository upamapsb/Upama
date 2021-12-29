import pandas as pd

from cowidev.testing import CountryTestBase


class Canada(CountryTestBase):
    source_url = "https://health-infobase.canada.ca/src/data/covidLive/covid19-download.csv"
    source_url_ref = "https://health-infobase.canada.ca/src/data/covidLive/covid19-download.csv"
    location = "Canada"
    rename_columns = {
        "numtests": "Cumulative total",
        "numtested": "Cumulative total",
        "prname": "Country",
        "date": "Date",
    }
    source_label = "Government of Canada"

    def read(self):
        df = pd.read_csv(self.source_url, usecols=["prname", "date", "numtested", "numtests"])
        df = df[df.prname == "Canada"]
        return df

    def pipeline_metric(self, df: pd.DataFrame, units: str, metric_field: str) -> pd.DataFrame:
        if metric_field == "numtested":
            metric_drop = "numtests"
        elif metric_field == "numtests":
            metric_drop = "numtested"
        else:
            raise ValueError("Invalid metric")
        df = (
            df.copy()
            .assign(Units=units)
            .drop(columns=[metric_drop])
            .pipe(self.pipe_rename_columns)
            .sort_values("Date")
            .drop_duplicates(subset=["Cumulative total"], keep="last")
        )
        df = df[~df["Cumulative total"].isna()]
        return df

    def pipeline(self, df: pd.DataFrame, units: str, metric_field: str) -> pd.DataFrame:
        return df.pipe(self.pipeline_metric, units, metric_field).pipe(self.pipe_metadata)

    def export(self):
        df = self.read()  # .pipe(self.pipeline_base)
        # People
        df_ = df.pipe(self.pipeline, "people tested", "numtested")
        self.export_datafile(df_, filename=f"{self.location} - people tested")
        # Tests
        df_ = df.pipe(self.pipeline, "tests performed", "numtests")
        self.export_datafile(df_, filename=f"{self.location} - tests performed")


def main():
    Canada().export()


if __name__ == "__main__":
    main()
