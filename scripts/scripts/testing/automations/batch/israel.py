import pandas as pd


from cowidev.testing import CountryTestBase
from cowidev.utils import clean_date_series


class Israel(CountryTestBase):
    location = "Israel"
    source_url = "https://datadashboardapi.health.gov.il/api/queries/testsPerDate"
    units = "tests performed"
    source_label = "Israel Ministry of Health"
    source_url_ref = source_url
    rename_columns = {
        "date": "Date",
        "numResultsForVirusDiagnosis": "Daily change in cumulative total",
    }

    def read(self):
        df = pd.read_json(self.source_url)[["date", "numResultsForVirusDiagnosis"]]
        return df

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.pipe(self.pipe_rename_columns).pipe(self.pipe_metadata)
        df = df.assign(Date=clean_date_series(df.Date))
        return df

    def export(self):
        df = self.read().pipe(self.pipeline)
        self.export_datafile(df)


def main():
    Israel().export()


if __name__ == "__main__":
    main()
