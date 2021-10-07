import re

from bs4 import BeautifulSoup
import pandas as pd

from cowidev.utils.clean import clean_count, clean_date
from cowidev.utils.web.scraping import get_soup
from cowidev.vax.utils.incremental import enrich_data, increment


class CostaRica:
    location: str = "Costa Rica"
    source_url: str = "https://www.ccss.sa.cr/web/coronavirus/vacunacion"

    def read(self) -> pd.Series:
        soup = get_soup(self.source_url)
        return self._parse_data(soup)

    def _parse_data(self, soup: BeautifulSoup) -> pd.Series:
        total_vaccinations = self._parse_total_vaccinations(soup)
        people_vaccinated, people_fully_vaccinated = self._parse_people_vaccinated(soup)
        assert people_vaccinated >= people_fully_vaccinated
        assert total_vaccinations >= people_vaccinated

        data = {
            "total_vaccinations": total_vaccinations,
            "people_vaccinated": people_vaccinated,
            "people_fully_vaccinated": people_fully_vaccinated,
            "date": self._parse_date(soup),
        }
        return pd.Series(data=data)

    def _parse_total_vaccinations(self, soup):
        r = "Total de dosis aplicadas"
        elems = soup.find_all(class_="counter")
        if r in elems[0].parent.text:
            return clean_count(elems[0])
        raise ValueError("`total_vaccinations` not found!")

    def _parse_people_vaccinated(self, soup):
        elem = soup.find("h4", string="Primera dosis").parent
        text = elem.find(class_="cifra1 bg3").text
        people_vaccinated = clean_count(text)
        elem = soup.find("h4", string="Segunda dosis").parent
        text = elem.find(class_="cifra1 bg4").text
        people_fully_vaccinated = clean_count(text)
        return people_vaccinated, people_fully_vaccinated

    def _parse_date(self, soup):
        date = soup.find(class_="actualiza").text
        date = re.search(r"\d{2}-\d{2}-\d{4}", date).group(0)
        date = clean_date(date, "%d-%m-%Y")
        return date

    def pipe_location(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "location", self.location)

    def pipe_vaccine(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "vaccine", "Oxford/AstraZeneca, Pfizer/BioNTech")

    def pipe_source(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "source_url", self.source_url)

    def pipeline(self, ds: pd.Series) -> pd.Series:
        return ds.pipe(self.pipe_location).pipe(self.pipe_vaccine).pipe(self.pipe_source)

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
    CostaRica().export(paths)
