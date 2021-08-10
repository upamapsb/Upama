import pandas as pd

from cowidev.vax.utils.incremental import clean_count, enrich_data, increment
from cowidev.vax.utils.utils import get_soup
from cowidev.vax.utils.dates import localdate


class Bolivia:
    def __init__(self):
        self.source_url = "https://www.unidoscontraelcovid.gob.bo/"
        self.location = "Bolivia"

    def read(self):
        soup = get_soup(self.source_url)
        return self._parse_data(soup)

    def _parse_data(self, soup):
        elem = soup.find(class_="vacunometro-cifras").parent
        ds = pd.read_html(str(elem), header=1)[0].squeeze()
        # Format check
        if ds.shape != (2,):
            raise ValueError("New cell added!")

        if ds.index.difference(["PRIMERA DOSIS", "SEGUNDA DOSIS"]).any():
            raise ValueError("Unknown cell detected or new cell added!")
        # Index renaming
        ds = ds.rename({
            "PRIMERA DOSIS": "people_vaccinated",
            "SEGUNDA DOSIS": "people_fully_vaccinated",
        })
        ds = ds.apply(clean_count)
        ds = enrich_data(ds, "date", localdate("America/La_Paz"))
        return ds

    def pipe_location(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "location", self.location)

    def pipe_vaccine(self, ds: pd.Series) -> pd.Series:
        return enrich_data(
            ds,
            "vaccine",
            "Oxford/AstraZeneca, Pfizer/BioNTech, Sinopharm/Beijing, Sputnik V",
        )

    def pipe_vaccinations(self, ds: pd.Series) -> pd.Series:
        total_vaccinations = ds.people_vaccinated + ds.people_fully_vaccinated
        return enrich_data(ds, "total_vaccinations", total_vaccinations)

    def pipe_source(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "source_url", self.source_url)

    def pipeline(self, ds: pd.Series) -> pd.Series:
        return (
            ds.pipe(self.pipe_location)
            .pipe(self.pipe_vaccine)
            .pipe(self.pipe_source)
            .pipe(self.pipe_vaccinations)
        )

    def export(self, paths):
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
    Bolivia().export(paths)


if __name__ == "__main__":
    main()
