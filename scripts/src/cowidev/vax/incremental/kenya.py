import os
import tempfile
import re

import requests
import pandas as pd
import PyPDF2

from cowidev.utils.clean import clean_count
from cowidev.vax.utils.incremental import merge_with_current_data
from cowidev.utils.web import get_soup
from cowidev.utils import paths


class Kenya:
    def __init__(self):
        self.location = "Kenya"
        self.source_url = "https://www.health.go.ke"
        self.output_file = os.path.join(f"{paths.SCRIPTS.OUTPUT_VAX_MAIN}", f"{self.location}.csv")
        self.last_update = self.get_last_update()

    def read(self):
        links = self._get_list_pdf_urls()
        records = []
        for link in links:
            pages = self._get_text_from_pdf(link)
            date = self._parse_date(pages[0])
            print(date, link)
            if date <= self.last_update:
                break
            total_vaccinations, people_vaccinated, people_fully_vaccinated, booster_doses = self._parse_metrics(pages)
            records.append(
                {
                    "total_vaccinations": total_vaccinations,
                    "people_vaccinated": people_vaccinated,
                    "people_fully_vaccinated": people_fully_vaccinated,
                    "total_boosters": booster_doses,
                    "date": date,
                    "source_url": link,
                }
            )
        assert len(records) > 0, f"No new record found after {self.last_update}"
        return pd.DataFrame(records)

    def _get_list_pdf_urls(self):
        soup = get_soup(self.source_url, verify=False)
        links = list(
            map(lambda x: x.get("href"), soup.findAll("a", text=re.compile("MINISTRY OF HEALTH KENYA COVID-19")))
        )
        return links

    def _get_text_from_pdf(self, url_pdf: str) -> str:
        def _extract_pdf_text(reader, n):
            page = reader.getPage(n)
            text = page.extractText().replace("\n", "")
            text = " ".join(text.split()).lower()
            return text

        with tempfile.NamedTemporaryFile() as tf:
            with open(tf.name, mode="wb") as f:
                f.write(requests.get(url_pdf, verify=False).content)
            with open(tf.name, mode="rb") as f:
                reader = PyPDF2.PdfFileReader(f)
                pages = [_extract_pdf_text(reader, n) for n in range(reader.numPages)]
        return pages

    def _parse_date(self, pdf_text: str):
        regex = r"vaccine doses dispensed as at (day )?[a-z]+ ([0-9]+.{0,2},? [a-z]+,? 202\d)"
        date_str = re.search(regex, pdf_text).group(2)
        date = str(pd.to_datetime(date_str).date())
        return date

    def _parse_metrics(self, pages: list):
        regex = (
            r"total doses administered ([\d,.]+) "
            r"doses administered above 18 ye?a?rs(?:\(primary schedule\))? ([\d,.]+) "
            r"partially vaccinated above 18 ye?a?rs ([\d,.]+) "
            r"fully vaccinated above 18 yrs ([\d,.]+)"
            r".*"
            r"doses administered 15-18yrs ([\d,.]+) "
            r"booster doses ([\d,.]+)"
        )
        data = re.search(regex, pages[0])
        total_vaccinations = clean_count(data.group(1))
        total_vaccinations_adults = clean_count(data.group(2))
        partially_vaccinated_adults = clean_count(data.group(3))
        fully_vaccinated_adults = clean_count(data.group(4))
        total_vaccinations_teenagers = clean_count(data.group(5))
        booster_doses = clean_count(data.group(6))

        if not total_vaccinations_adults + total_vaccinations_teenagers + booster_doses == total_vaccinations:
            raise ValueError(
                f"Assumptions do not hold. Assumptions made were: "
                r"1) doses administered 15-18 yrs = first doses 15-18 yrs"
            )

        # Correct people vaccinated with JJ doses and add teenager data
        people_vaccinated = partially_vaccinated_adults + self._extract_jj_doses(pages) + total_vaccinations_teenagers

        return total_vaccinations, people_vaccinated, fully_vaccinated_adults, booster_doses

    def _extract_jj_doses(self, pages):
        rex_header = (
            r"priority group johnson & johnson dose 2 uptake total fully vaccinated \(j&j \+ dose 2 uptake\) partially"
            r" vaccinated \(dose 1 uptake\) % dose 2 uptake"
        )
        rex_jj = r"total ([\d,]+) (?:[\d,]+) (?:[\d,]+) (?:[\d,]+) (?:[\d.]+)% table 5 shows percentage of clients"
        for page in pages:
            if "table 5: fully vaccinated vs. partially vaccinated above 18 years by priority group" in page:
                if not re.search(rex_header, page):
                    raise Exception("Header columns of table 5 have changed!")
                doses_jj = re.search(rex_jj, page).group(1)
        return clean_count(doses_jj)

    def pipe_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(location=self.location, vaccine="Oxford/AstraZeneca, Sputnik V")

    def pipeline(self, ds: pd.Series) -> pd.Series:
        return ds.pipe(self.pipe_metadata)

    def get_last_update(self):
        return pd.read_csv(self.output_file).date.max()

    def export(self):
        df = self.read().pipe(self.pipeline)
        df = merge_with_current_data(df, self.output_file)
        df.to_csv(self.output_file, index=False)


def main():
    Kenya().export()


if __name__ == "__main__":

    main()
