import pandas as pd

from cowidev.utils import get_soup, clean_date_series
from cowidev.testing import CountryTestBase


class Cyprus(CountryTestBase):
    location: str = "Cyprus"
    source_url: str = "https://www.data.gov.cy/node/4617?language=en"

    def read(self):
        soup = get_soup(self.source_url)
        url = soup.find_all(class_="data-link")[-1]["href"]
        df = pd.read_csv(url, usecols=["date", "total tests"])
        return df

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        # Rename
        df = df.rename(
            columns={
                "date": "Date",
                "total tests": "Cumulative total",
            }
        )
        # Remove NaNs
        df = df[~df["Cumulative total"].isna()]
        # Date
        df = df.assign(
            **{
                "Date": clean_date_series(df.Date, "%d/%m/%Y"),
                "Country": self.location,
                "Source label": "Ministry of Health",
                "Source URL": self.source_url,
                "Units": "tests performed",
                "Notes": pd.NA,
            }
        )
        return df

    def export(self):
        df = self.read().pipe(self.pipeline)
        self.export_datafile(df, attach=True)


def main():
    Cyprus().export()


if __name__ == "__main__":
    main()
