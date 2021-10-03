import tempfile
import re
from datetime import datetime

import requests
import pandas as pd
import PyPDF2
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

from cowidev.utils.clean import clean_count, clean_date
from cowidev.vax.utils.incremental import enrich_data, increment


class Nepal:
    def __init__(self):
        self.location = "Nepal"
        self.source_url = "https://covid19.mohp.gov.np/"

    def read(self):
        url_pdf = self._parse_pdf_link(self.source_url)
        pdf_text = self._get_text_from_pdf(url_pdf)
        people_vaccinated, people_fully_vaccinated = self._parse_metrics(pdf_text)
        return pd.Series(
            {
                "people_vaccinated": people_vaccinated,
                "people_fully_vaccinated": people_fully_vaccinated,
                "date": self._parse_date(pdf_text),
                "source_url": url_pdf,
            }
        )

    def _parse_pdf_link(self, url: str) -> str:
        op = Options()
        op.add_argument("--headless")
        with webdriver.Chrome(options=op) as driver:
            driver.set_page_load_timeout(20)
            driver.get(url)
            a = driver.find_elements_by_tag_name("a")
            a = [aa for aa in a if aa.text == "Download Situation Report"]
            if len(a) > 1:
                raise Exception("Format changed")
            url_pdf = a[0].get_attribute("href")
        return url_pdf

    def _get_text_from_pdf(self, url_pdf: str) -> str:
        with tempfile.NamedTemporaryFile() as tf:
            with open(tf.name, mode="wb") as f:
                f.write(requests.get(url_pdf).content)
            with open(tf.name, mode="rb") as f:
                reader = PyPDF2.PdfFileReader(f)
                page = reader.getPage(0)
                text = page.extractText().replace("\n", "")
        return text

    def _parse_date(self, pdf_text: str):
        regex = r"(\d{1,2})\s+([A-Za-z]+)\s+(202\d)"
        day = clean_count(re.search(regex, pdf_text).group(1))
        month = self._get_month(re.search(regex, pdf_text).group(2))
        year = clean_count(re.search(regex, pdf_text).group(3))
        return clean_date(datetime(year, month, day))

    def _parse_metrics(self, pdf_text: str):
        regex = r"1st Dose\s+\|\s+Fully Vaccinated +(\d+) +\([\d\.]+\) +(\d+)"
        data = re.search(regex, pdf_text)
        people_vaccinated = clean_count(data.group(1))
        people_fully_vaccinated = clean_count(data.group(2))
        return people_vaccinated, people_fully_vaccinated

    def _get_month(self, month_raw: str):
        months_dix = {
            "January": 1,
            "February": 2,
            "March": 3,
            "April": 4,
            "May": 5,
            "June": 6,
            "July": 7,
            "August": 8,
            "September": 9,
            "October": 10,
            "November": 11,
            "December": 12,
        }
        for month_name, month_id in months_dix.items():
            if month_name in month_raw:
                return month_id

    def pipe_location(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "location", self.location)

    def pipe_vaccine(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "vaccine", "Johnson&Johnson, Oxford/AstraZeneca, Sinopharm/Beijing")

    def pipe_metrics(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "total_vaccinations", ds.people_vaccinated + ds.people_fully_vaccinated)

    def pipeline(self, ds: pd.Series) -> pd.Series:
        return ds.pipe(self.pipe_location).pipe(self.pipe_vaccine).pipe(self.pipe_metrics)

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
    Nepal().to_csv(paths)
