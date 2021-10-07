import re

import pandas as pd

from cowidev.utils.clean import clean_count, extract_clean_date
from cowidev.utils.web.scraping import get_soup
from cowidev.vax.utils.incremental import enrich_data, increment


class EquatorialGuinea:
    def __init__(self):
        self.source_url = "https://guineasalud.org/estadisticas/"
        self.location = "Equatorial Guinea"

    def read(self) -> pd.Series:
        soup = get_soup(self.source_url)
        data = self.parse_metrics(soup)
        date_str = self.parse_date(soup)
        return pd.Series(
            {
                **data,
                "date": date_str,
            }
        )

    def parse_metrics(self, soup):
        # Get key points table (total_vaccinations, people_vaccinated)
        dfs = pd.read_html(self.source_url)
        df = dfs[0].rename(columns={"Unnamed: 0": "metric"}).set_index("metric")
        people_vaccinated = clean_count(df.loc["Total Vacunados 1ª dosis", "Totales"])
        total_vaccinations = clean_count(df.loc["Total dosis administradas", "Totales"])
        # Get people_fully_vaccinated from text
        regex = (
            r"De los ([\d\.]+) vacunados,? un total de ([\d\.]+)\s?\([\d,]+%\) ya (han recibido la 2ª dosis|tienen la"
            r" pauta completa)"
        )
        match = re.search(regex, soup.text)
        people_fully_vaccinated = clean_count(match.group(2))
        # Sanity check
        if people_vaccinated != clean_count(match.group(1)):
            raise ValueError(
                f"There is an error! First dose metrics appears with different values in the source website!"
            )
        # Build data
        return {
            "total_vaccinations": total_vaccinations,
            "people_vaccinated": people_vaccinated,
            "people_fully_vaccinated": people_fully_vaccinated,
        }

    def parse_date(self, soup):
        return extract_clean_date(
            text=soup.text,
            regex=r"Datos: a (\d+ \w+ de 20\d{2})",
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
        return ds.pipe(self.pipe_location).pipe(self.pipe_vaccine).pipe(self.pipe_source)

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
    EquatorialGuinea().to_csv(paths)


if __name__ == "__main__":
    main()
