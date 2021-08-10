import pandas as pd

from cowidev.vax.utils.incremental import enrich_data, increment
from cowidev.vax.utils.dates import localdate


def read(source: str) -> pd.Series:
    df = pd.read_csv(
        source,
        usecols=[
            "primera_dosis_cantidad",
            "segunda_dosis_cantidad",
            "vacuna_nombre",
        ],
    )

    known_vaccines = set(
        (
            "AstraZeneca ChAdOx1 S recombinante",
            "COVISHIELD ChAdOx1nCoV COVID 19",
            "Sinopharm Vacuna SARSCOV 2 inactivada",
            "Sputnik V COVID19 Instituto Gamaleya",
            "Moderna ARNm",
        )
    )
    assert set(df.vacuna_nombre) == known_vaccines, "New vaccine found!"

    return df.drop(columns="vacuna_nombre").sum()


def translate_index(ds: pd.Series) -> pd.Series:
    return ds.rename(
        {
            "primera_dosis_cantidad": "people_vaccinated",
            "segunda_dosis_cantidad": "people_fully_vaccinated",
        }
    )


def add_totals(ds: pd.Series) -> pd.Series:
    total_vaccinations = ds["people_vaccinated"] + ds["people_fully_vaccinated"]
    return enrich_data(ds, "total_vaccinations", total_vaccinations)


def format_date(ds: pd.Series) -> pd.Series:
    date = localdate("America/Argentina/Buenos_Aires", 8)
    return enrich_data(ds, "date", date)


def enrich_location(ds: pd.Series) -> pd.Series:
    return enrich_data(ds, "location", "Argentina")


def enrich_vaccine(ds: pd.Series) -> pd.Series:
    return enrich_data(
        ds, "vaccine", "Moderna, Oxford/AstraZeneca, Sinopharm/Beijing, Sputnik V"
    )


def enrich_source(ds: pd.Series) -> pd.Series:
    return enrich_data(
        ds,
        "source_url",
        "http://datos.salud.gob.ar/dataset/vacunas-contra-covid-19-dosis-aplicadas-en-la-republica-argentina",
    )


def pipeline(ds: pd.Series) -> pd.Series:
    return (
        ds.pipe(translate_index)
        .pipe(add_totals)
        .pipe(format_date)
        .pipe(enrich_location)
        .pipe(enrich_vaccine)
        .pipe(enrich_source)
    )


def main(paths):
    source = "https://sisa.msal.gov.ar/datos/descargas/covid-19/files/Covid19VacunasAgrupadas.csv.zip"
    data = read(source).pipe(pipeline)
    increment(
        paths=paths,
        location=str(data["location"]),
        total_vaccinations=int(data["total_vaccinations"]),
        people_vaccinated=int(data["people_vaccinated"]),
        people_fully_vaccinated=int(data["people_fully_vaccinated"]),
        date=str(data["date"]),
        source_url=str(data["source_url"]),
        vaccine=str(data["vaccine"]),
    )


if __name__ == "__main__":
    main()
