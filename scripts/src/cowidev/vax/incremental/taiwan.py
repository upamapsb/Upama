import re
import math
import pandas as pd
import tabula

from bs4 import BeautifulSoup

from cowidev.utils.clean import clean_count, clean_date
from cowidev.utils.web.scraping import get_soup, get_response
from cowidev.vax.utils.incremental import enrich_data, increment


class Taiwan:
    source_url = "https://www.cdc.gov.tw"
    location = "Taiwan"
    vaccines_mapping = {
        "AstraZeneca": "Oxford/AstraZeneca",
        "高端": "Medigen",
        "Moderna": "Moderna",
        "BioNTech": "Pfizer/BioNTech",
    }

    @property
    def source_data_url(self):
        return f"{self.source_url}/Category/Page/9jFXNbCe-sFK9EImRRi2Og"

    def read(self) -> pd.Series:
        soup = get_soup(self.source_data_url)
        url_pdf = self._parse_pdf_link(soup)
        df = self._parse_table(url_pdf)
        data = self.parse_data(df, soup)
        return data

    def _parse_pdf_link(self, soup) -> str:
        for a in soup.find(class_="download").find_all("a"):
            if "疫苗接種統計資料" in a["title"]:
                break
        url_pdf = f"{self.source_url}{a['href']}"
        for i in range(10):
            response = get_response(url_pdf)
            if response.headers["Content-Type"] == "application/pdf":
                return url_pdf
            content = response.content
            soup = BeautifulSoup(content, "lxml", from_encoding=None)
            a = soup.find(class_="viewer-button")
            if a is not None:
                break
        return f"{self.source_url}{a['href']}"

    def _parse_table(self, url_pdf: str):
        dfs = self._parse_tables_all(url_pdf)
        df = dfs[0]
        cols = df.columns

        if df.shape != (20, 4):
            raise ValueError(f"Table 1: format has changed!")

        # Sanity check
        if not (
            len(cols) == 4
            and cols[0] == "廠牌"
            and cols[1] == "劑次"
            and cols[2].endswith("接種人次")
            and re.match(r"(\d+/\d+\/\d+ *\- *)?(\d+/(\d+\/)?)?\d+? *接種人次", cols[2])
            and re.match(r"累計至 *\d+/\d+\/\d+ *接種人次", cols[3])
        ):
            raise ValueError(f"There are some unknown columns: {cols}")

        if df.iloc[16][0] != "總計":
            raise ValueError(f"Unexpected value in the key cell: {df.iloc[16][0]}")

        # The last few columns may be left-shifted and require this small surgery.
        # If math.isnan() raise exception that means the table is changed.
        for i in range(17, 20):
            if math.isnan(df.iloc[i][3]):
                df.iloc[i][[3, 2, 1]] = df.iloc[i][[2, 1, 0]]
                df.iloc[i][0] = float("nan")

        df["劑次"] = df["劑次"].str.replace("\s+", "", regex=True)
        df["廠牌"] = df["廠牌"].fillna(method="ffill")
        df = df.set_index(["廠牌", "劑次"])
        df.columns = ["daily", "total"]
        return df

    def _parse_tables_all(self, url_pdf: str) -> int:
        kwargs = {"pandas_options": {"dtype": str, "header": 0}, "lattice": True}
        dfs = tabula.read_pdf(url_pdf, pages=1, **kwargs)
        return dfs

    def parse_data(self, df: pd.DataFrame, soup):
        stats = self._parse_stats(df)
        data = pd.Series(
            {
                "total_boosters": stats["total_boosters"],
                "total_vaccinations": stats["total_vaccinations"],
                "people_vaccinated": stats["people_vaccinated"],
                "people_fully_vaccinated": stats["people_fully_vaccinated"],
                "date": self._parse_date(soup),
                "vaccine": self._parse_vaccines(df),
            }
        )
        return data

    def _parse_stats(self, df: pd.DataFrame) -> int:
        num_dose1 = clean_count(df.loc["總計", "第1劑"]["total"])
        num_dose2 = clean_count(df.loc["總計", "第2劑"]["total"])
        num_booster1 = clean_count(df.loc["總計", "基礎加強劑"]["total"])
        num_booster2 = clean_count(df.loc["總計", "追加劑"]["total"])

        return {
            "total_vaccinations": num_dose1 + num_dose2 + num_booster1 + num_booster2,
            "people_vaccinated": num_dose1,
            "people_fully_vaccinated": num_dose2,
            "total_boosters": num_booster1 + num_booster2,
        }

    def _parse_vaccines(self, df: pd.DataFrame) -> str:
        vaccines = set(df.index.levels[0]) - {"總計", "追加劑"}
        vaccines_wrong = vaccines.difference(self.vaccines_mapping)
        if vaccines_wrong:
            raise ValueError(f"Invalid vaccines: {vaccines_wrong}")
        return ", ".join(sorted(self.vaccines_mapping[vax] for vax in vaccines))

    def _parse_date(self, soup) -> str:
        date_raw = soup.find(class_="download").text
        regex = r"(\d{4})\sCOVID-19疫苗"
        date_str = re.search(regex, date_raw).group(1)
        date_str = clean_date(f"2022{date_str}", fmt="%Y%m%d")
        return date_str

    def pipe_location(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "location", self.location)

    def pipe_source(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "source_url", self.source_data_url)

    def pipeline(self, ds: pd.Series) -> pd.Series:
        return ds.pipe(self.pipe_location).pipe(self.pipe_source)

    def export(self):
        data = self.read().pipe(self.pipeline)
        increment(
            location=data["location"],
            total_vaccinations=data["total_vaccinations"],
            people_vaccinated=data["people_vaccinated"],
            people_fully_vaccinated=data["people_fully_vaccinated"],
            date=data["date"],
            source_url=data["source_url"],
            vaccine=data["vaccine"],
            total_boosters=data["total_boosters"],
        )


def main():
    Taiwan().export()


if __name__ == "__main__":
    main()
