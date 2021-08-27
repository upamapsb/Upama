import datetime

import pandas as pd

from cowidev.vax.utils.incremental import enrich_data, increment, clean_count
from cowidev.vax.utils.dates import clean_date
from cowidev.vax.utils.utils import get_soup


def read(source: str) -> pd.Series:
    soup = get_soup(source)

    counters = soup.find_all(class_="text-brand-blue")
    dose_1 = clean_count(counters[1].text)
    dose_2 = clean_count(counters[2].text)
    assert dose_1 >= dose_2

    date = soup.find(class_="text-gray-500").text
    date = date + str(datetime.date.today().year)
    date = clean_date(date, fmt="Nutarterneqarpoq: %d. %B%Y", lang="en")

    return pd.Series({"people_vaccinated": dose_1, "people_fully_vaccinated": dose_2, "date": date})


def enrich_source(ds: pd.Series, source: str) -> pd.Series:
    return enrich_data(ds, "source_url", source)


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "vaccine", "Moderna")


def enrich_location(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "location", "Greenland")


def add_totals(ds: pd.Series) -> pd.Series:
    total_vaccinations = ds["people_vaccinated"] + ds["people_fully_vaccinated"]
    return enrich_data(ds, "total_vaccinations", total_vaccinations)


def pipeline(ds: pd.Series, source: str) -> pd.Series:
    return ds.pipe(enrich_location).pipe(enrich_vaccine).pipe(enrich_source, source).pipe(add_totals)


def main(paths):
    source = "https://corona.nun.gl"
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
