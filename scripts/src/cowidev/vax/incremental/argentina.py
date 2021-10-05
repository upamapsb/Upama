import pandas as pd

from cowidev.vax.utils.incremental import enrich_data, increment
from cowidev.utils.clean.dates import localdate


class Argentina:
    location: str = "Argentina"
    source_url_ref: str = (
        "http://datos.salud.gob.ar/dataset/vacunas-contra-covid-19-dosis-aplicadas-en-la-republica-argentina"
    )
    source_url: str = "https://sisa.msal.gov.ar/datos/descargas/covid-19/files/Covid19VacunasAgrupadas.csv.zip"
    vaccine_mapping: dict = {
        "AstraZeneca ChAdOx1 S recombinante": "Oxford/AstraZeneca",
        "Cansino Ad5 nCoV": "CanSino",
        "COVISHIELD ChAdOx1nCoV COVID 19": "Oxford/AstraZeneca",
        "Moderna ARNm": "Moderna",
        "Pfizer BioNTech Comirnaty": "Pfizer/BioNTech",
        "Sinopharm Vacuna SARSCOV 2 inactivada": "Sinopharm/Beijing",
        "Sputnik V COVID19 Instituto Gamaleya": "Sputnik V",
        # "Vacuna COVID Estudios Clínicos"
    }
    one_dose_vaccine: list = ["Cansino Ad5 nCoV"]

    def read(self) -> pd.DataFrame:
        return pd.read_csv(
            self.source_url,
            usecols=[
                "primera_dosis_cantidad",
                "segunda_dosis_cantidad",
                "vacuna_nombre",
            ],
        )

    def pipe_vaccine_checks(self, df: pd.DataFrame) -> pd.DataFrame:
        vaccines_known = {*set(self.vaccine_mapping)}  # , "Vacuna COVID Estudios Clínicos"}
        vaccines_unknown = set(df.vacuna_nombre).difference(vaccines_known)
        if vaccines_unknown:
            raise ValueError(f"Unknown vaccine name(s): {vaccines_unknown}")
        return df

    def pipe_metrics(self, df: pd.DataFrame) -> pd.Series:
        mask = df.vacuna_nombre.isin(self.one_dose_vaccine)
        df_1d = df.loc[mask]
        df_nd = df.loc[~mask]
        single_shots = df_1d["primera_dosis_cantidad"].sum()
        first_shots = df_nd["primera_dosis_cantidad"].sum()
        second_shots = df_nd["segunda_dosis_cantidad"].sum()

        if df_1d["segunda_dosis_cantidad"].sum() != 0:
            raise ValueError(
                f"`segunda_dosis_cantidad` field for one dose vaccines ({self.one_dose_vaccine}) must be 0!"
            )
        vaccines = set(df.vacuna_nombre.replace(self.vaccine_mapping))
        # vaccines.remove("Vacuna COVID Estudios Clínicos")
        print(vaccines)
        vaccines = ", ".join(sorted(vaccines))
        return pd.Series(
            {
                "people_vaccinated": first_shots + single_shots,
                "people_fully_vaccinated": second_shots + single_shots,
                "total_vaccinations": df["primera_dosis_cantidad"].sum() + df["segunda_dosis_cantidad"].sum(),
                "vaccine": vaccines,
            }
        )

    def pipe_date(self, ds: pd.Series) -> pd.Series:
        date = localdate("America/Argentina/Buenos_Aires", 8)
        return enrich_data(ds, "date", date)

    def pipe_location(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "location", self.location)

    def pipe_source(self, ds: pd.Series) -> pd.Series:
        return enrich_data(
            ds,
            "source_url",
            self.source_url_ref,
        )

    def pipeline(self, ds: pd.Series) -> pd.Series:
        return (
            ds.pipe(self.pipe_vaccine_checks)
            .pipe(self.pipe_metrics)
            .pipe(self.pipe_date)
            .pipe(self.pipe_location)
            .pipe(self.pipe_source)
        )

    def export(self, paths):
        data = self.read().pipe(self.pipeline)
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


def main(paths):
    Argentina().export(paths)
