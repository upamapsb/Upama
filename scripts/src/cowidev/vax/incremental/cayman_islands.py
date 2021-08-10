import re
import requests

from bs4 import BeautifulSoup
import pandas as pd

from cowidev.vax.utils.incremental import enrich_data, increment, clean_count
from cowidev.vax.utils.dates import localdate


def read(source: str) -> pd.Series:
    soup = BeautifulSoup(requests.get(source).content, "html.parser")
    data = parse_data(soup)
    return enrich_data(data, "source_url", source)


def parse_data(soup: BeautifulSoup) -> pd.Series:
    regex_1 = r"([\d,]+) C(ovid|OVID)-19 vaccinations has been given in total in the Cayman Islands"
    regex_1 = (
        r"([\d,]+) C(ovid|OVID)-19 vaccinations given in total in the Cayman Islands"
    )
    total_vaccinations = clean_count(re.search(regex_1, soup.text).group(1))

    # regex_2 = (
    #     r"Of these, ([\d,]+) \((?:[\d,]+)% of (?:[a-zA-Z0-9,]+)\) have had at least one dose"
    # )
    # assert total_vaccinations >= people_vaccinated
    # people_fully_vaccinated = total_vaccinations - people_vaccinated
    regex_2 = (
        r"Of these,? ([\d,]+) \((?:[\d,]+)% of (?:an estimated )?([\d,]+)\) (?:have )?had at least one dose of a "
        r"C(?:ovid|OVID)-19 vaccine and ([\d,]+) \(([\d,]+)%\) (have )?completed the two-dose course"
    )

    matches = re.search(regex_2, soup.text)
    people_vaccinated = clean_count(matches.group(1))
    population = clean_count(matches.group(2))
    people_fully_vaccinated = clean_count(matches.group(3))

    return pd.Series(
        {
            "total_vaccinations": total_vaccinations,
            "people_vaccinated": people_vaccinated,
            "people_fully_vaccinated": people_fully_vaccinated,
        }
    )


def set_date(ds: pd.Series) -> pd.Series:
    date = localdate("America/Cayman")
    return enrich_data(ds, "date", date)


def enrich_location(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "location", "Cayman Islands")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "vaccine", "Oxford/AstraZeneca, Pfizer/BioNTech")


def pipeline(ds: pd.Series) -> pd.Series:
    return ds.pipe(set_date).pipe(enrich_location).pipe(enrich_vaccine)


def main(paths):
    source = "https://www.exploregov.ky/coronavirus-statistics"
    data = read(source).pipe(pipeline)
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


if __name__ == "__main__":
    main()
