import re
import requests

from bs4 import BeautifulSoup
import pandas as pd

from cowidev.vax.utils.incremental import enrich_data, increment, clean_count
from cowidev.vax.utils.utils import get_soup
from cowidev.vax.utils.dates import clean_date


class Singapore:
    def __init__(self) -> None:
        self.location = "Singapore"
        self.source_url = "https://www.moh.gov.sg/covid-19/vaccination"

    def read(self) -> pd.Series:
        soup = get_soup(self.source_url)
        return self._parse_data(soup)

    def _parse_data(self, soup: BeautifulSoup) -> pd.Series:
        data = pd.Series(
            {
                "date": self._parse_date(soup),
                "total_vaccinations": self._parse_metric(
                    soup, "Total Doses Administered"
                ),
                "people_vaccinated": self._parse_metric(
                    soup, "Received at least First Dose"
                ),
                "people_fully_vaccinated": self._parse_metric(
                    soup, "Completed Full Vaccination Regimen"
                ),
            }
        )
        return data

    def _parse_date(self, soup: BeautifulSoup) -> str:
        for h3 in soup.find_all("h3"):
            if "Vaccination Data" in h3.text:
                break
        date = re.search(r"as of (\d+ \w+ \d+)", h3.text).group(1)
        date = str(pd.to_datetime(date).date())
        return date

    def _parse_metric(self, soup: BeautifulSoup, description: str) -> int:
        value = (
            soup.find("strong", string=description)
            .parent.parent.parent.parent.find_all("tr")[-1]
            .text
        )
        return clean_count(value)

    def pipe_location(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "location", "Singapore")

    def pipe_vaccine(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "vaccine", "Moderna, Pfizer/BioNTech")

    def pipe_source(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "source_url", self.source_url)

    def pipeline(self, ds: pd.Series) -> pd.Series:
        return (
            ds.pipe(self.pipe_location).pipe(self.pipe_source).pipe(self.pipe_vaccine)
        )

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
    Singapore().to_csv(paths)
