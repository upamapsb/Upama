import re

from bs4 import BeautifulSoup
import pandas as pd

from cowidev.utils.clean import clean_count
from cowidev.utils.clean.dates import localdate
from cowidev.utils.web import get_soup
from cowidev.vax.utils.incremental import enrich_data, increment


class CaymanIslands:
    location: str = "Cayman Islands"
    source_url: str = "https://www.exploregov.ky/coronavirus-statistics"
    regex: dict = {
        "people_vaccinated": r"([\d,]+) \(.*\) have had at least one dose of a COVID-19 vaccine",
        "people_fully_vaccinated": r"([\d,]+) \(.*\) have completed the two-dose course",
    }

    def read(self) -> pd.Series:
        soup = get_soup(self.source_url)
        data = self._parse_data(soup)
        return pd.Series(data=data)

    def _parse_data(self, soup: BeautifulSoup) -> pd.Series:
        people_vaccinated = self._parse_metric(soup, "people_vaccinated")
        people_fully_vaccinated = self._parse_metric(soup, "people_fully_vaccinated")
        total_vaccinations = people_vaccinated + people_fully_vaccinated
        return {
            "total_vaccinations": total_vaccinations,
            "people_vaccinated": people_vaccinated,
            "people_fully_vaccinated": people_fully_vaccinated,
        }

    def _parse_metric(self, soup, metric_name):
        return clean_count(
            re.search(
                self.regex[metric_name],
                soup.text,
            ).group(1)
        )

    def pipe_date(self, ds: pd.Series) -> pd.Series:
        date = localdate("America/Cayman")
        return enrich_data(ds, "date", date)

    def pipe_source(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "source_url", self.source_url)

    def pipe_location(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "location", self.location)

    def pipe_vaccine(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "vaccine", "Oxford/AstraZeneca, Pfizer/BioNTech")

    def pipeline(self, ds: pd.Series) -> pd.Series:
        return ds.pipe(self.pipe_date).pipe(self.pipe_location).pipe(self.pipe_source).pipe(self.pipe_vaccine)

    def export(self, paths):
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
    CaymanIslands().export(paths)
