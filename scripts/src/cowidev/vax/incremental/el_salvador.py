import json

import pandas as pd
from bs4 import BeautifulSoup

from cowidev.utils.clean import clean_count, extract_clean_date
from cowidev.utils.web.scraping import get_soup
from cowidev.vax.utils.incremental import enrich_data, increment


def read(source: str) -> pd.Series:
    soup = get_soup(source)
    link = parse_infogram_link(soup)
    soup = get_soup(link)
    infogram_data = parse_infogram_data(soup)
    return pd.Series(
        {
            "date": parse_infogram_date(infogram_data),
            "source_url": source,
            **parse_infogram_vaccinations(infogram_data),
        }
    )


def parse_infogram_link(soup: BeautifulSoup) -> str:
    url_end = soup.find(class_="infogram-embed").get("data-id")
    return f"https://e.infogram.com/{url_end}"


def parse_infogram_data(soup: BeautifulSoup) -> dict:
    for script in soup.find_all("script"):
        if "infographicData" in str(script):
            json_data = script.string[:-1].replace("window.infographicData=", "")
            json_data = json.loads(json_data)
            break
    json_data = json_data["elements"]["content"]["content"]["entities"]
    return json_data


def _get_infogram_value(infogram_data: dict, field_id: str, join_text: bool = False):
    if join_text:
        return "".join(x["text"] for x in infogram_data[field_id]["props"]["content"]["blocks"])
    return infogram_data[field_id]["props"]["content"]["blocks"][0]["text"]


def parse_infogram_vaccinations(infogram_data: dict) -> int:
    total_vaccinations = clean_count(_get_infogram_value(infogram_data, "5088d5fc-24f7-46db-bf7e-3234db46a262"))
    people_vaccinated = clean_count(_get_infogram_value(infogram_data, "90b218fc-f246-4a2e-bc33-fa0af726fb67"))
    people_fully_vaccinated = clean_count(_get_infogram_value(infogram_data, "efc94320-fe88-4d58-abb6-4d703c5983dc"))
    total_boosters = clean_count(_get_infogram_value(infogram_data, "12ece579-eb52-4622-b412-4c152c3fa457"))
    return {
        "total_vaccinations": total_vaccinations,
        "people_vaccinated": people_vaccinated,
        "people_fully_vaccinated": people_fully_vaccinated,
        "total_boosters": total_boosters,
    }


def parse_infogram_date(infogram_data: dict) -> str:
    x = _get_infogram_value(infogram_data, "3f6fa939-ad51-436f-a5dd-d6952609d242", join_text=True)
    dt = extract_clean_date(x, "RESUMEN DE VACUNACIÓN\s?(\d+-[A-Z]+-2\d)\s?", "%d-%b-%y", lang="es")
    return dt


def enrich_location(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "location", "El Salvador")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "vaccine", "Oxford/AstraZeneca, Pfizer/BioNTech, Sinopharm/Beijing, Sinovac")


def pipeline(ds: pd.Series) -> pd.Series:
    return ds.pipe(enrich_location).pipe(enrich_vaccine)


def main(paths):
    source = "https://covid19.gob.sv/"
    data = read(source).pipe(pipeline)
    increment(
        paths=paths,
        location=data["location"],
        total_vaccinations=data["total_vaccinations"],
        people_vaccinated=data["people_vaccinated"],
        people_fully_vaccinated=data["people_fully_vaccinated"],
        total_boosters=data["total_boosters"],
        date=data["date"],
        source_url=data["source_url"],
        vaccine=data["vaccine"],
    )


if __name__ == "__main__":
    main()
