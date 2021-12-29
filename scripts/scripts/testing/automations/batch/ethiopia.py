"""Constructs daily time series of COVID-19 testing data for Ethiopia.

Dashboard: https://www.covid19.et/covid-19/Home/DataPresentationByTable

"""

import datetime
import pandas as pd


from cowidev.testing import CountryTestBase
from cowidev.utils.clean import extract_clean_date

MAX_TRIES = 5


# sample of official values for cross-checking against the scraped data.
sample_official_data = [
    (
        "2020-09-18",
        {"Cumulative total": 1184473, "source": "https://twitter.com/EPHIEthiopia/status/1306992815730307073"},
    ),
    (
        "2020-09-11",
        {"Cumulative total": 1122659, "source": "https://twitter.com/EPHIEthiopia/status/1304467982630948864"},
    ),
    (
        "2020-07-19",
        {
            "Cumulative total": 331266,
            "source": "https://www.ephi.gov.et/images/novel_coronavirus/EPHI_-PHEOC_COVID-19_Weekly-bulletin_12_English_07202020.pdf",
        },
    ),
    (
        "2020-06-13",
        {
            "Cumulative total": 176504,
            "source": (
                "https://www.ephi.gov.et/images/novel_coronavirus/confirmed-case-Press-release_June-13-Eng_V2.pdf"
            ),
        },
    ),
    (
        "2020-05-30",
        {
            "Cumulative total": 106615,
            "source": (
                "https://www.ephi.gov.et/images/novel_coronavirus/confirmed-case-Press-release_May-30--Eng_V1.pdf"
            ),
        },
    ),
    (
        "2020-05-11",
        {
            "Cumulative total": 36624,
            "source": (
                "https://www.ephi.gov.et/images/novel_coronavirus/confirmed-case-Press-release_May-11_-ENG-V1-1.pdf"
            ),
        },
    ),
    (
        "2020-04-28",
        {
            "Cumulative total": 15668,
            "source": (
                "https://www.ephi.gov.et/images/novel_coronavirus/confirmed-case-Press-release_April-28_-ENG-V1-1.pdf"
            ),
        },
    ),
    (
        "2020-04-25",
        {
            "Cumulative total": 12688,
            "source": (
                "https://www.ephi.gov.et/images/novel_coronavirus/confirmed-case-Press-release_April-25_-ENG-V5-1.pdf"
            ),
        },
    ),
    (
        "2020-03-31",
        {
            "Cumulative total": 1013,
            "source": "https://www.ephi.gov.et/images/novel_coronavirus/Press-release_March-31_2020_Eng_TA.pdf",
        },
    ),
    (
        "2020-03-22",
        {"Cumulative total": 392, "source": "https://twitter.com/lia_tadesse/status/1241610916736663558"},
    ),
]


class Ethiopia(CountryTestBase):
    location = "Ethiopia"
    units = "tests performed"
    source_label = "Ethiopia Information Network Security Agency"
    source_url = "https://www.covid19.et/covid-19/Home/DataPresentationByTable"
    source_url_ref = source_url

    def export(self) -> None:
        df = self.read(MAX_TRIES)
        df = df.pipe(self.pipeline)
        df.loc[:, "Source URL"] = df["Source URL"].apply(lambda x: self.source_url_ref if pd.isnull(x) else x)
        df = df.pipe(self.pipe_metadata)
        sanity_checks(df)
        self.export_datafile(df)

    def read(self, max_trials=MAX_TRIES):
        df = None
        i = 0
        while df is None and i < max_trials:
            dfs = pd.read_html(self.source_url, attrs={"id": "dataTable"})
            assert len(dfs) > 0, f'Failed to find table with id="dataTable" at {self.source_url}'
            df = dfs[0].copy()
            assert df.shape[0] > 100, (
                "Expected table to have > 100 rows, but table "
                f"only has {df.shape[0]} rows. If this problem "
                "persists, you may need to use selenium to change"
                "the number of rows that are displayed on the page."
            )
        assert df is not None, (
            f"Failed to retrieve testing data after {i} tries. "
            "If this problem persists, check that the URL "
            f"is working ({self.source_url})."
        )
        return df

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        # Column rename
        df = df.rename(
            columns={
                df.columns[0]: "Date",
                df.columns[1]: "Daily change in cumulative total",
                df.columns[2]: "Cumulative total",
            },
        )
        # Date
        df["Date"] = df["Date"].apply(lambda x: extract_clean_date(x, r"(\d+\/\d+\/20\d+).*", date_format="%d/%m/%Y"))
        # Date filter
        df = df.sort_values(["Date", "Cumulative total"]).drop_duplicates(subset=["Date"], keep="last")
        df = df[-df["Date"].isin(["2020-03-18", "2021-07-04"])]

        df = df.dropna(subset=["Date", "Cumulative total"], how="any")[["Date", "Cumulative total"]]

        df["Cumulative total"] = df["Cumulative total"].astype(int)
        df = df[df["Cumulative total"] > 0]
        df = df.groupby("Cumulative total", as_index=False).min()
        df = df.groupby("Date", as_index=False).min()
        df.loc[:, "Source URL"] = None
        return df


def sanity_checks(df: pd.DataFrame) -> None:
    """checks that there are no obvious errors in the scraped data."""
    df_temp = df.copy()
    # checks that the max date is less than tomorrow's date.
    assert datetime.datetime.strptime(df_temp["Date"].max(), "%Y-%m-%d") < (
        datetime.datetime.utcnow() + datetime.timedelta(days=1)
    )
    # checks that there are no duplicate dates
    assert df_temp["Date"].duplicated().sum() == 0, "One or more rows share the same date."
    if "Cumulative total" not in df_temp.columns:
        df_temp["Cumulative total"] = df_temp["Daily change in cumulative total"].cumsum()
    # checks that the cumulative number of tests on date t is always greater than the figure for t-1:
    assert (
        df_temp["Cumulative total"].iloc[1:] >= df_temp["Cumulative total"].shift(1).iloc[1:]
    ).all(), "On one or more dates, `Cumulative total` is greater on date t-1."
    # df.iloc[1:][df['Cumulative total'].iloc[1:] < df['Cumulative total'].shift(1).iloc[1:]]
    # cross-checks a sample of scraped figures against the expected result.
    assert len(sample_official_data) > 0
    for dt, d in sample_official_data:
        val = df_temp.loc[df_temp["Date"] == dt, "Cumulative total"].squeeze().sum()
        assert (
            val == d["Cumulative total"]
        ), f"scraped value ({val:,d}) != official value ({d['Cumulative total']:,d}) on {dt}"
    return None


def main():
    Ethiopia().export()


if __name__ == "__main__":
    main()
