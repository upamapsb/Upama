import pandas as pd

from cowidev.utils.clean.dates import localdate
from cowidev.vax.utils.incremental import enrich_data, increment


def read(source: str) -> pd.Series:
    data = pd.read_csv(source, sep=";")
    return parse_data(data).pipe(enrich_data, "date", get_date())


def parse_data(df: pd.DataFrame) -> pd.Series:

    df = df[df.Age == "All ages"]

    dose_1 = df.loc[df["Vaccination dose"] == "First dose", "val"].item()
    dose_2 = df.loc[df["Vaccination dose"] == "Second dose", "val"].item()
    dose_3 = df.loc[df["Vaccination dose"] == "Third dose", "val"].item()

    return pd.Series(
        {
            "people_vaccinated": dose_1,
            "people_fully_vaccinated": dose_2,
            "total_vaccinations": dose_1 + dose_2 + dose_3,
            "total_boosters": dose_3,
        }
    )


def get_date() -> str:
    return localdate("Europe/Helsinki")


def enrich_location(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "location", "Finland")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "vaccine", "Moderna, Oxford/AstraZeneca, Pfizer/BioNTech")


def enrich_source(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "source_url", "https://sampo.thl.fi/pivot/prod/en/vaccreg/cov19cov")


def pipeline(ds: pd.Series) -> pd.Series:
    return ds.pipe(enrich_location).pipe(enrich_vaccine).pipe(enrich_source)


def main():
    source = "https://sampo.thl.fi/pivot/prod/en/vaccreg/cov19cov/fact_cov19cov.csv?row=cov_vac_dose-533170.533164.639082.&column=cov_vac_age-630311.518413.&filter=measure-533175&"
    data = read(source).pipe(pipeline)
    increment(
        location=data["location"],
        total_vaccinations=int(data["total_vaccinations"]),
        people_vaccinated=int(data["people_vaccinated"]),
        people_fully_vaccinated=int(data["people_fully_vaccinated"]),
        total_boosters=int(data["total_boosters"]),
        date=data["date"],
        source_url=data["source_url"],
        vaccine=data["vaccine"],
    )


if __name__ == "__main__":
    main()
