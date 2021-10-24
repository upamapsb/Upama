import requests
from datetime import datetime

import pandas as pd
from bs4 import BeautifulSoup

from utils import make_monotonic


class BosniaHerzegovina:
    def __init__(self):
        self.location = "Bosnia and Herzegovina"
        self.source_url = [
            "http://mcp.gov.ba/publication/read/epidemioloska-slika-covid-19?pageId=3",
            "http://mcp.gov.ba/publication/read/epidemioloska-slika-novo?pageId=97",
        ]

    def read(self):
        dfs = [self._load_data(url) for url in self.source_url]
        df = pd.concat(dfs)
        return df

    def _load_data(self, url: str):
        df = pd.DataFrame(self._get_records(url))
        df = df[~df["Cumulative total"].isna()]
        df = df.assign(**{"Source URL": url})
        return df

    def _get_records(self, url: str) -> dict:
        soup = BeautifulSoup(requests.get(url).content, "html.parser")
        elem = soup.find(id="newsContent")
        elems = elem.find_all("table")
        records = [
            {
                "Date": self._parse_date(elem),
                "Cumulative total": self._parse_metric(elem),
            }
            for elem in elems
        ]
        return records

    def _parse_metric(self, elem):
        df = pd.read_html(str(elem), header=1)[0]
        value = df.loc[df["Unnamed: 0"] == "BiH", "Broj testiranih"].item()
        return value

    def _parse_date(self, elem):
        return datetime.strptime(elem.find("p").text.strip(), "%d.%m.%Y.").strftime("%Y-%m-%d")

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.assign(
            **{
                "Country": self.location,
                "Source label": "Ministry of Civil Affairs",
                "Units": "tests performed",
                "Notes": pd.NA,
            }
        ).sort_values("Date")
        df.loc[:, "Cumulative total"] = (
            df.loc[:, "Cumulative total"].astype(str).str.replace(r"\s|\*", "", regex=True).astype(int)
        )
        df = df.pipe(self._remove_typo)
        df = df.drop_duplicates(subset="Date", keep=False)
        return df

    def _remove_typo(self, df: pd.DataFrame) -> pd.DataFrame:
        if (df.Date == "2021-01-08").sum() == 2:
            ds = abs(df.loc[df.Date == "2021-01-08", "Cumulative total"] - 535439)
            id_remove = ds.idxmax()
            df = df.drop(id_remove)
        df = df[df.Date != "2021-08-23"]
        return df

    def to_csv(self):
        output_path = f"automated_sheets/{self.location}.csv"
        df = self.read().pipe(self.pipeline).pipe(make_monotonic)
        df.to_csv(output_path, index=False)


def main():
    BosniaHerzegovina().to_csv()


if __name__ == "__main__":
    main()
