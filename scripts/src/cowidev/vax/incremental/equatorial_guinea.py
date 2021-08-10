import re

import pandas as pd

from cowidev.vax.utils.incremental import enrich_data, increment, clean_count
from cowidev.vax.utils.utils import get_soup
from cowidev.vax.utils.dates import extract_clean_date


class EquatorialGuinea:
    def __init__(self, source_url: str, location: str, columns_rename: dict = None):
        self.source_url = source_url
        self.location = location

    def read(self) -> pd.Series:
        soup = get_soup(self.source_url)
        people_vaccinated, people_fully_vaccinated = self.parse_vaccinated(soup)
        date_str = self.parse_date(soup)
        data = pd.Series(
            {
                "people_vaccinated": people_vaccinated,
                "people_fully_vaccinated": people_fully_vaccinated,
                "total_vaccinations": people_vaccinated + people_fully_vaccinated,
                "date": date_str,
            }
        )
        return pd.Series(data)

    def parse_vaccinated(self, soup):
        regex = r"De los ([\d\.]+) vacunados un total de ([\d\.]+) \(([\d\.,]+)%\) ya han recibido la 2ª dosis"
        match = re.search(regex, soup.text)
        people_vaccinated = match.group(1)
        people_fully_vaccinated = match.group(2)
        return clean_count(people_vaccinated), clean_count(people_fully_vaccinated)

    def parse_date(self, soup):
        return extract_clean_date(
            text=soup.text,
            regex=r"Datos:\s{1,2}a (\d{1,2} [a-zA-Z]+ de 202\d)",
            date_format="%d %B de %Y",
            lang="es",
            unicode_norm=True,
        )

    def pipe_location(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "location", self.location)

    def pipe_vaccine(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "vaccine", "Sinopharm/Beijing")

    def pipe_source(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "source_url", self.source_url)

    def pipeline(self, ds: pd.Series) -> pd.Series:
        return (
            ds.pipe(self.pipe_location).pipe(self.pipe_vaccine).pipe(self.pipe_source)
        )

    def to_csv(self, paths):
        """Generalized."""
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
    EquatorialGuinea(
        source_url="https://guineasalud.org/estadisticas/",
        location="Equatorial Guinea",
    ).to_csv(paths)


if __name__ == "__main__":
    main()
