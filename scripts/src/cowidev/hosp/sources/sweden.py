import re

import pandas as pd

from cowidev.utils.web.scraping import get_soup

METADATA = {
    "source_url_ref": "https://www.folkhalsomyndigheten.se/smittskydd-beredskap/utbrott/aktuella-utbrott/covid-19/statistik-och-analyser/analys-och-prognoser/",
    "source_name": "Swedish Public Health Agency",
    "entity": "Sweden",
}


def main() -> pd.DataFrame:
    soup = get_soup(METADATA["source_url_ref"])

    file_url = soup.find(
        class_="xlsx", text=re.compile(".*Underlag för Vårdbelastning aktuell beläggning över tid.*")
    ).get("href")
    file_url = "https://www.folkhalsomyndigheten.se" + file_url

    hosp = pd.read_excel(file_url, sheet_name="Slutenvård Total", usecols=["Datum", "Riket"]).rename(
        columns={"Riket": "hospital_stock", "Datum": "date"}
    )

    icu = pd.read_excel(file_url, sheet_name="inom intensivvårdsavdelning", usecols=["Datum", "Riket"]).rename(
        columns={"Riket": "icu_stock", "Datum": "date"}
    )

    df = (
        pd.merge(hosp, icu, on="date", how="outer", validate="one_to_one")
        .melt("date", var_name="indicator")
        .dropna(subset=["value"])
    )
    df["indicator"] = df.indicator.replace(
        {
            "hospital_stock": "Daily hospital occupancy",
            "icu_stock": "Daily ICU occupancy",
        }
    )

    df["entity"] = METADATA["entity"]

    return df, METADATA


if __name__ == "__main__":
    main()
