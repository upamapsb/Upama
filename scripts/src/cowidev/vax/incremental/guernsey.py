import pandas as pd

from cowidev.utils.clean import extract_clean_date
from cowidev.utils.web.scraping import get_soup
from cowidev.vax.utils.incremental import enrich_data, increment


class Guernsey:
    def __init__(self, source_url: str, location: str):
        self.source_url = source_url
        self.location = location
        self._regex_date = r"This page was last updated on (\d{1,2} [A-Za-z]+ 202\d)"

    def read(self) -> pd.Series:
        soup = get_soup(self.source_url)
        return self.parse_data(soup)

    def parse_data(self, soup):
        # Get table
        tables = soup.find_all("table")
        ds = pd.read_html(str(tables[0]))[0].squeeze()
        # Rename, add/remove columns
        ds = ds.rename({"Total doses": "total_vaccinations"})
        ds["date"] = extract_clean_date(
            text=str(soup.text),
            regex=self._regex_date,
            date_format="%d %B %Y",
            lang="en",
        )
        return ds.loc[["date", "total_vaccinations"]]

    def pipe_location(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "location", self.location)

    def pipe_vaccine(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "vaccine", "Moderna, Oxford/AstraZeneca, Pfizer/BioNTech")

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
            date=data["date"],
            source_url=data["source_url"],
            vaccine=data["vaccine"],
        )


def main(paths):
    Guernsey(
        source_url="https://covid19.gov.gg/guidance/vaccine/stats",
        location="Guernsey",
    ).to_csv(paths)


if __name__ == "__main__":
    main()
