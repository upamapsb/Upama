import re

from bs4 import BeautifulSoup
import pandas as pd

from vax.utils.utils import get_soup
from vax.utils.incremental import clean_count, enrich_data, increment
from vax.utils.dates import clean_date


class Serbia:

    def __init__(self):
        self.location = "Serbia"
        self.source_url = "https://vakcinacija.gov.rs/"
        self.regex = {
            "metrics": r"Обе дозе вакцине примило је ([\d.]+) особа. Укупно вакцинација: ([\d.]+) доза",
            "date": r"ажурирано .*",
            "total_vaccinations": r"Укупан број датих доза: ([\d.]+)",
            "people_vaccinated": (
                r"Укупан број грађана који су примили прву дозу и оних којих су примили обе дозе: ([\d.]+)"
            ),
            "people_fully_vaccinated": r"Укупан број грађана који су примили обе дозе: ([\d.]+)",
        }

    def read(self) -> pd.Series:
        soup = get_soup(self.source_url)
        total_vaccinations, people_vaccinated, people_fully_vaccinated = self._parse_metrics(soup)
        return pd.Series({
            "total_vaccinations": total_vaccinations,
            "people_vaccinated": people_vaccinated,
            "people_fully_vaccinated": people_fully_vaccinated,
            "source_url": self.source_url,
            "date": self._parse_date(soup)
        })

    def _parse_metrics(self, soup: BeautifulSoup):
        total_vaccinations = clean_count(
            re.search(self.regex["total_vaccinations"], soup.text).group(1)
        )
        people_vaccinated = clean_count(
            re.search(self.regex["people_vaccinated"], soup.text).group(1)
        )
        people_fully_vaccinated = clean_count(
            re.search(self.regex["people_fully_vaccinated"], soup.text).group(1)
        )
        return total_vaccinations, people_vaccinated, people_fully_vaccinated

    def _parse_metrics_old(self, soup: BeautifulSoup):
        match = re.search(self.regex["metrics"], soup.text)
        total_vaccinations = clean_count(match.group(2))
        people_fully_vaccinated = clean_count(match.group(1))
        return total_vaccinations, people_fully_vaccinated

    def _parse_date(self, soup: BeautifulSoup) -> str:
        elems = soup.find_all("p")
        x = []
        for elem in elems:
            if elem.find(text=re.compile(self.regex["date"])):
                x.append(elem)
        if len(x) > 1:
            raise ValueError("Format of source has changed")
        date_str = clean_date(x[0].text, "ажурирано %d.%m.%Y")
        return date_str

    def pipe_metrics(self, ds: pd.Series) -> pd.Series:
        people_vaccinated = ds.total_vaccinations - ds.people_fully_vaccinated
        return enrich_data(ds, "people_vaccinated", people_vaccinated)

    def pipe_location(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "location", self.location)

    def pipe_vaccine(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "vaccine", "Oxford/AstraZeneca, Pfizer/BioNTech, Sinopharm/Beijing, Sputnik V")

    def pipeline(self, ds: pd.Series) -> pd.Series:
        return (
            ds
            .pipe(self.pipe_vaccine)
            .pipe(self.pipe_location)
            # .pipe(self.pipe_metrics)
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
            vaccine=data["vaccine"]
        )


def main(paths):
    Serbia().to_csv(paths)


if __name__ == "__main__":
    main()
