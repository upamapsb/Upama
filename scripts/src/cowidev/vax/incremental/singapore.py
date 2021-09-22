import re

from bs4 import BeautifulSoup
import pandas as pd

from cowidev.utils.clean import clean_count, clean_date
from cowidev.utils.web import get_soup
from cowidev.vax.utils.incremental import enrich_data, increment


class Singapore:
    def __init__(self) -> None:
        self.location = "Singapore"
        self.feed_url = "https://www.moh.gov.sg/feeds/news-highlights"

    def find_article(self) -> str:
        soup = get_soup(self.feed_url)
        for link in soup.find_all("item"):
            elements = link.children
            for elem in elements:
                if "local-covid-19-situation" in elem:
                    return elem

    def read(self) -> pd.Series:
        self.source_url = self.find_article()
        # print(self.source_url)
        soup = get_soup(self.source_url)
        return self.parse_text(soup)

    def parse_text(self, soup: BeautifulSoup) -> pd.Series:

        preamble = (
            r"As of ([\d]+ [A-Za-z]+ 20\d{2}), (\d+)% of our population has completed their full regimen/"
            r" received two doses of COVID-19 vaccines, and (\d+)% has received at least one dose\."
        )
        data = re.search(preamble, soup.text).groups()
        date = clean_date(data[0], fmt="%d %B %Y", lang="en_US", loc="en_US")
        share_fully_vaccinated = int(data[1])
        share_vaccinated = int(data[2])

        national_program = (
            r"We have administered a total of ([\d,]+) doses of COVID-19 vaccines under the national vaccination programme"
            r" \(Pfizer-BioNTech Comirnaty and Moderna\), covering ([\d,]+) individuals"
        )
        data = re.search(national_program, soup.text).groups()
        national_doses = clean_count(data[0])
        national_people_vaccinated = clean_count(data[1])

        who_eul = (
            r"In addition, ([\d,]+) doses of other vaccines recognised in the World Health Organization.s Emergency"
            r" Use Listing \(WHO EUL\) have been administered, covering ([\d,]+) individuals\."
        )
        data = re.search(who_eul, soup.text).groups()
        who_doses = clean_count(data[0])
        who_people_vaccinated = clean_count(data[1])

        total_vaccinations = national_doses + who_doses
        people_vaccinated = national_people_vaccinated + who_people_vaccinated
        people_fully_vaccinated = round(people_vaccinated * (share_fully_vaccinated / share_vaccinated))

        data = pd.Series(
            {
                "date": date,
                "total_vaccinations": total_vaccinations,
                "people_vaccinated": people_vaccinated,
                "people_fully_vaccinated": people_fully_vaccinated,
            }
        )
        return data

    def pipe_location(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "location", "Singapore")

    def pipe_vaccine(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "vaccine", "Moderna, Pfizer/BioNTech, Sinovac")

    def pipe_source(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "source_url", self.source_url)

    def pipeline(self, ds: pd.Series) -> pd.Series:
        return ds.pipe(self.pipe_location).pipe(self.pipe_source).pipe(self.pipe_vaccine)

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
