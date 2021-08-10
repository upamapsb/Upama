import re

import pandas as pd

from cowidev.vax.utils.incremental import enrich_data, increment, clean_count
from cowidev.vax.utils.utils import get_soup
from cowidev.vax.utils.dates import clean_date


def read(source: str) -> pd.Series:
    soup = get_soup(source)

    counters = soup.find_all(class_="counter")
    people_vaccinated = clean_count(counters[0].text)
    total_vaccinations = clean_count(counters[2].text)

    date = soup.find("span", id="last-update").text
    date = re.search(r"\d+.*202\d", date).group(0)
    date = str((pd.to_datetime(date) - pd.Timedelta(days=1)).date())

    data = {
        "total_vaccinations": total_vaccinations,
        "people_vaccinated": people_vaccinated,
        "people_fully_vaccinated": None,
        "date": date,
        "source_url": source,
    }

    return pd.Series(data=data)


def enrich_location(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "location", "Pakistan")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return enrich_data(
        ds,
        "vaccine",
        "CanSino, Oxford/AstraZeneca, Sinovac, Sinopharm/Beijing, Sputnik V",
    )


def pipeline(ds: pd.Series) -> pd.Series:
    return ds.pipe(enrich_location).pipe(enrich_vaccine)


def main(paths):
    source = "https://ncoc.gov.pk/covid-vaccination-en.php"
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
