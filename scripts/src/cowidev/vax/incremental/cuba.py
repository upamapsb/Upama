import re
from datetime import datetime

from bs4 import BeautifulSoup
import pandas as pd

from cowidev.utils import clean_count, clean_date, get_soup
from cowidev.utils.clean.dates import localdate
from cowidev.vax.utils.incremental import increment, enrich_data


class Cuba:
    def __init__(self):
        self.source_url = "https://salud.msp.gob.cu/actualizacion-de-la-vacunacion-en-el-marco-de-los-estudios-de-los-candidatos-vacunales-cubanos-y-la-intervencion-sanitaria/"
        self.location = "Cuba"
        self.regex = {
            "title": r"Al cierre del (\d{1,2}(?:ro)? de [a-z]+) se acumulan en el país ([\d ]+) dosis administradas",
            "people_vaccinated": r"al menos una dosis [^\.]+, ([\d ]+) personas",
            "people_fully_vaccinated": r"Tienen esquema de vacunación completo ([\d ]+) personas",
            "total_boosters": r"Cuentan con dosis de refuerzo un total de ([\d ]+) personas",
        }

    def read(self) -> pd.Series:
        soup = get_soup(self.source_url)
        return self._parse_data(soup)

    def _parse_data(self, soup: BeautifulSoup) -> pd.Series:
        return pd.Series(
            data={
                "date": self._parse_date(soup),
                **self._parse_metrics(soup),
            }
        )

    def _parse_date(self, soup):
        match = re.search(self.regex["title"], soup.text)
        date_str = match.group(1).replace("ro", "")
        date = clean_date(f"{date_str} {datetime.now().year}", "%d de %b %Y", lang="es")
        if date > localdate("America/Havana", force_today=True):
            date = clean_date(f"{date_str} {datetime.now().year-1}", "%d de %b %Y", lang="es")
        return date

    def _parse_metrics(self, soup):
        match = re.search(self.regex["title"], soup.text)
        data = {"total_vaccinations": clean_count(match.group(2))}

        for metric in ["people_vaccinated", "people_fully_vaccinated", "total_boosters"]:
            match = re.search(self.regex[metric], soup.text)
            data[metric] = clean_count(match.group(1))
        return data

    def pipe_vaccine(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "vaccine", "Abdala, Soberana02")

    def pipeline(self, df: pd.Series) -> pd.Series:
        return df.pipe(self.pipe_vaccine)

    def to_csv(self):
        data = self.read().pipe(self.pipeline)
        increment(
            location=self.location,
            total_vaccinations=data["total_vaccinations"],
            people_vaccinated=data["people_vaccinated"],
            people_fully_vaccinated=data["people_fully_vaccinated"],
            total_boosters=data["total_boosters"],
            date=data["date"],
            source_url=self.source_url,
            vaccine=data["vaccine"],
        )


def main():
    Cuba().to_csv()
