import re
import requests

from bs4 import BeautifulSoup
import pandas as pd

from cowidev.vax.utils.incremental import enrich_data, increment, clean_count
from cowidev.vax.utils.utils import get_soup
from cowidev.vax.utils.dates import clean_date


def read(source: str) -> pd.Series:
    soup = get_soup(source)
    return parse_data(soup)


def parse_data(soup: BeautifulSoup) -> pd.Series:

    people_vaccinated = clean_count(soup.find_all(class_="counter")[3].text)
    people_fully_vaccinated = clean_count(soup.find_all(class_="counter")[4].text)
    assert people_vaccinated >= people_fully_vaccinated
    total_vaccinations = people_vaccinated + people_fully_vaccinated

    date = soup.find(class_="actualiza").text
    date = re.search(r"\d{2}-\d{2}-\d{4}", date).group(0)
    date = clean_date(date, "%d-%m-%Y")

    data = {
        "total_vaccinations": total_vaccinations,
        "people_vaccinated": people_vaccinated,
        "people_fully_vaccinated": people_fully_vaccinated,
        "date": date,
    }
    return pd.Series(data=data)


def enrich_location(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "location", "Costa Rica")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "vaccine", "Oxford/AstraZeneca, Pfizer/BioNTech")


def enrich_source(ds: pd.Series, source: str) -> pd.Series:
    return enrich_data(ds, "source_url", source)


def pipeline(ds: pd.Series, source: str) -> pd.Series:
    return ds.pipe(enrich_location).pipe(enrich_vaccine).pipe(enrich_source, source)


def main(paths):
    source = "https://www.ccss.sa.cr/web/coronavirus/vacunacion"
    data = read(source).pipe(pipeline, source)
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
