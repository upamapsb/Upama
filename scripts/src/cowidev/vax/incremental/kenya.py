import tempfile
import re

import requests
import pandas as pd
import PyPDF2

from cowidev.utils.clean import clean_count
from cowidev.vax.utils.incremental import enrich_data, increment
from cowidev.utils.web import get_soup


class Kenya:
    def __init__(self):
        self.location = "Kenya"
        self.source_url = "https://www.health.go.ke"

    def read(self):
        url_pdf = self._parse_pdf_link(self.source_url)
        pdf_text = self._get_text_from_pdf(url_pdf)
        total_vaccinations, people_vaccinated, people_fully_vaccinated = self._parse_metrics(pdf_text)
        date = self._parse_date(pdf_text)
        return pd.Series(
            {
                "total_vaccinations": total_vaccinations,
                "people_vaccinated": people_vaccinated,
                "people_fully_vaccinated": people_fully_vaccinated,
                "date": date,
                "source_url": url_pdf,
            }
        )

    def _parse_pdf_link(self, url: str) -> str:
        soup = get_soup(url)
        url_pdf = soup.find("a", {"href": re.compile(".*IMMUNIZATION.*pdf$")})["href"]
        return url_pdf

    def _get_text_from_pdf(self, url_pdf: str) -> str:
        with tempfile.NamedTemporaryFile() as tf:
            with open(tf.name, mode="wb") as f:
                f.write(requests.get(url_pdf).content)
            with open(tf.name, mode="rb") as f:
                reader = PyPDF2.PdfFileReader(f)
                page = reader.getPage(0)
                text = page.extractText().replace("\n", "")
        text = " ".join(text.split()).lower()
        return text

    def _parse_date(self, pdf_text: str):
        regex = r"vaccine doses dispensed as at [a-z]+ ([0-9a-z]+, [a-z]+ 202\d)"
        date_str = re.search(regex, pdf_text).group(1)
        date = str(pd.to_datetime(date_str).date())
        return date

    def _parse_metrics(self, pdf_text: str):
        regex = (
            r"total doses administered ([\d,]+) total partially vaccinated ([\d,]+) total fully vaccinated ([\d,]+)"
        )
        data = re.search(regex, pdf_text)
        total_vaccinations = clean_count(data.group(1))
        people_vaccinated = clean_count(data.group(2))
        people_fully_vaccinated = clean_count(data.group(3))
        return total_vaccinations, people_vaccinated, people_fully_vaccinated

    def pipe_location(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "location", self.location)

    def pipe_vaccine(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "vaccine", "Oxford/AstraZeneca, Sputnik V")

    def pipeline(self, ds: pd.Series) -> pd.Series:
        return ds.pipe(self.pipe_location).pipe(self.pipe_vaccine)

    def to_csv(self, paths):
        data = self.read().pipe(self.pipeline)
        increment(
            paths=paths,
            location=data["location"],
            total_vaccinations=data["total_vaccinations"],
            people_vaccinated=data["people_vaccinated"],
            people_fully_vaccinated=data["people_fully_vaccinated"],
            date=data["date"],
            source_url=data["source_url"],
            vaccine=data["vaccine"],
        )


def main(paths):
    Kenya().to_csv(paths)
