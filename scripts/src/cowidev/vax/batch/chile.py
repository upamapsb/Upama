import pandas as pd

from cowidev.vax.utils.files import export_metadata


vaccine_mapping = {
    "Pfizer": "Pfizer/BioNTech",
    "Sinovac": "Sinovac",
    "Astra-Zeneca": "Oxford/AstraZeneca",
    "CanSino": "CanSino",
}
vaccines_one_dose = ["CanSino"]


class Chile:
    def __init__(self):
        self.location = "Chile"
        self.source_url = "https://raw.githubusercontent.com/MinCiencia/Datos-COVID19/master/output/producto76/fabricante.csv"
        self.source_url_ref = "https://www.gob.cl/yomevacuno/"

    def read(self) -> pd.DataFrame:
        return pd.read_csv(self.source_url)

    def read_age(self) -> pd.DataFrame:
        raise NotImplementedError("No source available")
        # return pd.read_csv(self.source_url_age)

    def pipe_melt(self, df: pd.DataFrame, id_vars: list) -> pd.DataFrame:
        return df.melt(id_vars, var_name="date", value_name="value")

    def pipe_filter_rows(self, df: pd.DataFrame, colname: str) -> pd.DataFrame:
        return df[(df[colname] != "Total") & (df.value > 0)]

    def pipe_pivot(self, df: pd.DataFrame, index: list) -> pd.DataFrame:
        return df.pivot(index=index, columns="Dosis", values="value").reset_index()

    def pipe_rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.rename(
            columns={
                "Primera": "people_vaccinated",
                "Segunda": "people_fully_vaccinated",
                "Fabricante": "vaccine",
            }
        )

    def pipe_rename_vaccines(self, df: pd.DataFrame) -> pd.DataFrame:
        vaccines_wrong = set(df["vaccine"].unique()).difference(vaccine_mapping)
        if vaccines_wrong:
            raise ValueError(f"Missing vaccines: {vaccines_wrong}")
        return df.replace(vaccine_mapping)

    def pipe_total_vaccinations(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(
            total_vaccinations=df.people_vaccinated.fillna(0)
            + df.people_fully_vaccinated.fillna(0)
        )

    def pipe_process_onedose_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.loc[df.vaccine.isin(vaccines_one_dose), "people_vaccinated"].sum() != 0:
            raise ValueError(
                f"Reporting of one dose vaccines changed! Check column `people_vaccinated` for these."
            )
        mask = df.vaccine.isin(vaccines_one_dose)
        df.loc[mask, "people_vaccinated"] = df.loc[mask, "people_fully_vaccinated"]
        return df

    def pipeline_base(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.pipe(self.pipe_melt, ["Fabricante", "Dosis"])
            .pipe(self.pipe_filter_rows, "Fabricante")
            .pipe(self.pipe_pivot, ["Fabricante", "date"])
            .pipe(self.pipe_rename_columns)
            .pipe(self.pipe_rename_vaccines)
            .pipe(self.pipe_total_vaccinations)
            .pipe(self.pipe_process_onedose_metrics)
        )

    def pipe_aggregate(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.sort_values("vaccine")
            .groupby("date", as_index=False)
            .agg(
                people_vaccinated=("people_vaccinated", "sum"),
                people_fully_vaccinated=("people_fully_vaccinated", "sum"),
                total_vaccinations=("total_vaccinations", "sum"),
                vaccine=("vaccine", ", ".join),
            )
            .assign(location=self.location)
        )

    def pipe_location(self, df: pd.DataFrame) -> pd.DataFrame:
        """Generalized."""
        return df.assign(location=self.location)

    def pipe_source(self, df: pd.DataFrame) -> pd.DataFrame:
        """Generalized."""
        return df.assign(source_url=self.source_url_ref)

    def pipeline_vaccinations(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.pipe(self.pipe_aggregate)
            .pipe(self.pipe_source)
            .sort_values(["location", "date"])
        )

    def pipeline_manufacturer(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(location=self.location)[
            ["location", "date", "vaccine", "total_vaccinations"]
        ].sort_values(["location", "date", "vaccine"])

    def pipe_postprocess_age(self, df: pd.DataFrame) -> pd.DataFrame:
        regex = r"(\d{1,2})(?:[ a-zA-Z]+|-(\d{1,2})[ a-zA-Z]*)"
        df[["age_group_min", "age_group_max"]] = df.Age.str.extract(regex)
        df = df[
            ["date", "age_group_min", "age_group_max", "total_vaccinations", "location"]
        ]
        return df

    def pipeline_age(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.pipe(self.pipe_melt, ["Age", "Dosis"])
            .pipe(self.pipe_filter_rows, "Age")
            .pipe(self.pipe_pivot, ["Age", "date"])
            .pipe(self.pipe_rename_columns)
            .pipe(self.pipe_total_vaccinations)
            .pipe(self.pipe_location)
            .pipe(self.pipe_postprocess_age)
            .sort_values(["location", "date", "age_group_min"])
        )

    def to_csv(self, paths):
        df = self.read().pipe(self.pipeline_base)
        # Main data
        df.pipe(self.pipeline_vaccinations).to_csv(
            paths.tmp_vax_out(self.location), index=False
        )
        # Manufacturer
        df_man = df.pipe(self.pipeline_manufacturer)
        df_man.to_csv(paths.tmp_vax_out_man(self.location), index=False)
        export_metadata(
            df_man,
            "Ministerio de Ciencia, Tecnología, Conocimiento e Innovación",
            self.source_url_ref,
            paths.tmp_vax_metadata_man,
        )
        # Age (commented because metrics are not relative to age group sizes)
        # df_age.to_csv(
        #     paths.tmp_vax_out_by_age_group(self.location),
        #     index=False
        # )


def main(paths):
    Chile().to_csv(paths)


if __name__ == "__main__":
    main()
