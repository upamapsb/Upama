import pandas as pd

from cowidev.utils.clean.dates import localdate, localdatenow
from cowidev.vax.utils.files import export_metadata_age
from cowidev.utils import paths


class Peru:
    def __init__(self) -> None:
        self.location = "Peru"
        self.source_url = (
            "https://github.com/jmcastagnetto/covid-19-peru-vacunas/raw/main/datos/vacunas_covid_resumen.csv"
        )
        self.source_url_age = (
            "https://github.com/jmcastagnetto/covid-19-peru-vacunas/raw/main/datos/vacunas_covid_rangoedad_owid.csv"
        )
        self.source_url_manufacturer = (
            "https://github.com/jmcastagnetto/covid-19-peru-vacunas/raw/main/datos/vacunas_covid_fabricante.csv"
        )
        self.source_url_ref = "https://www.datosabiertos.gob.pe/dataset/vacunacion"
        self.vaccine_mapping = {
            "SINOPHARM": "Sinopharm/Beijing",
            "PFIZER": "Pfizer/BioNTech",
            "ASTRAZENECA": "Oxford/AstraZeneca",
        }

    def read(self):
        return pd.read_csv(
            self.source_url,
            usecols=["fecha_vacunacion", "fabricante", "dosis", "n_reg"],
        )

    def read_age(self):
        return pd.read_csv(self.source_url_age)

    def read_manufacturer(self):
        return pd.read_csv(self.source_url_manufacturer)

    def pipe_rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.rename(columns={"fecha_vacunacion": "date", "fabricante": "vaccine"})
        return df.dropna(subset=["vaccine"])

    def pipe_checks(self, df: pd.DataFrame) -> pd.DataFrame:
        # Check vaccine names
        unknown_vaccines = set(df["vaccine"].unique()).difference(self.vaccine_mapping.keys())
        if unknown_vaccines:
            raise ValueError("Found unknown vaccines: {}".format(unknown_vaccines))
        # Check dose number
        dose_num_wrong = set(df.dosis).difference({1, 2, 3, 4})
        if dose_num_wrong:
            raise ValueError(f"Invalid dose number. Check field `dosis`: {dose_num_wrong}")
        return df

    def pipe_rename_vaccines(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.replace(self.vaccine_mapping)

    def pipe_format(self, df: pd.DataFrame) -> pd.DataFrame:
        df.loc[df.dosis >= 3, "dosis"] = 3  # All doses from 3 onwards are boosters
        return (
            df.drop(columns="vaccine")
            .groupby(["date", "dosis"], as_index=False)
            .sum()
            .pivot(index="date", columns="dosis", values="n_reg")
            .rename(columns={1: "people_vaccinated", 2: "people_fully_vaccinated", 3: "total_boosters"})
            .fillna(0)
            .sort_values("date")
            .cumsum()
            .reset_index()
        )

    def pipe_total_vaccinations(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(total_vaccinations=df.people_vaccinated + df.people_fully_vaccinated + df.total_boosters)

    def pipe_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(
            location=self.location,
            vaccine=", ".join(sorted(self.vaccine_mapping.values())),
            source_url=self.source_url_ref,
        )

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.pipe(self.pipe_rename_columns)
            .pipe(self.pipe_checks)
            .pipe(self.pipe_rename_vaccines)
            .pipe(self.pipe_format)
            .pipe(self.pipe_total_vaccinations)
            .pipe(self.pipe_metadata)
        )

    def pipe_age_checks(self, df: pd.DataFrame) -> pd.DataFrame:
        # print(df.columns)
        if (df.people_vaccinated_per_hundred > 100).sum():
            raise ValueError("Check `people_vaccinated_per_hundred` field! Found values above 100%.")
        if (df.people_fully_vaccinated_per_hundred > 100).sum():
            raise ValueError("Check `people_fully_vaccinated_per_hundred` field! Found values above 100%.")
        if (df["last_day_of_epi_week"].min() < "2021-02-14") or (
            df["last_day_of_epi_week"].max() > localdatenow("America/Lima", sum_days=7)
        ):
            raise ValueError(
                "Check `last_day_of_epi_week` field! Some dates may be out of normal! Current range is"
                f" [{df['last_day_of_epi_week'].min()}, {df['last_day_of_epi_week'].max()}]."
            )
        if not (df.location.unique() == "Peru").all():
            raise ValueError("Invalid values in `location` field!")
        return df

    def pipe_age_date(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.rename(columns={"last_day_of_epi_week": "date"})
        self._sanity_checks_age_date(df)
        df.loc[df.complete_epi_week == 0, "date"] = localdatenow("America/Lima")
        return df

    def _sanity_checks_age_date(self, df: pd.DataFrame):
        msk = df.date > localdatenow("America/Lima")
        if (
            (df.loc[msk, "complete_epi_week"].unique() != 0) | (df.loc[~msk, "complete_epi_week"].unique() != 1)
        ).any():
            raise ValueError(
                "Some inconsistency found! Check values for `last_day_of_epi_week` and `complete_epi_week`."
            )

    def pipe_columns_out(self, df: pd.DataFrame) -> pd.DataFrame:
        return df[
            [
                "location",
                "date",
                "age_group_min",
                "age_group_max",
                "people_vaccinated_per_hundred",
                "people_fully_vaccinated_per_hundred",
            ]
        ]

    def pipeline_age(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.pipe(self.pipe_age_checks).pipe(self.pipe_age_date).pipe(self.pipe_columns_out)

    def pipeline_manufacturer(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.sort_values(["location", "date", "vaccine"])[["location", "date", "vaccine", "total_vaccinations"]]
        if not df.groupby("vaccine")["total_vaccinations"].is_monotonic_increasing.all():
            raise ValueError("Manufacturer data for Peru is not monotonically increasing!")
        return df

    def export(self):
        df = self.read().pipe(self.pipeline)
        df.to_csv(paths.out_vax(self.location), index=False)
        # Age data
        df_age = self.read_age().pipe(self.pipeline_age)
        df_age.to_csv(paths.out_vax(self.location, age=True), index=False)
        export_metadata_age(
            df_age,
            "Ministerio de Salud via https://github.com/jmcastagnetto/covid-19-peru-vacunas",
            self.source_url_ref,
        )
        # Manufacturer data
        df_manuf = self.read_manufacturer().pipe(self.pipeline_manufacturer)
        df_manuf.to_csv(paths.out_vax(self.location, manufacturer=True), index=False)
        export_metadata_age(
            df_manuf,
            "Ministerio de Salud via https://github.com/jmcastagnetto/covid-19-peru-vacunas",
            self.source_url_ref,
        )


def main():
    Peru().export()
