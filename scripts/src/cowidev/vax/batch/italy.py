import pandas as pd

from cowidev.vax.utils.files import export_metadata

LOCATION = "Italy"
SOURCE_URL = (
    "https://raw.githubusercontent.com/italia/covid19-opendata-vaccini/master/dati/somministrazioni-vaccini-latest.csv"
)
COLUMNS_RENAME = {
    "data_somministrazione": "date",
    "fornitore": "vaccine",
    "fascia_anagrafica": "age_group",
    "dose_aggiuntiva": "total_boosters",
}
VACCINE_MAPPING = {
    "Pfizer/BioNTech": "Pfizer/BioNTech",
    "Moderna": "Moderna",
    "Vaxzevria (AstraZeneca)": "Oxford/AstraZeneca",
    "Janssen": "Johnson&Johnson",
}
ONE_DOSE_VACCINES = ["Johnson&Johnson"]


class Italy:
    def __init__(
        self,
        source_url: str,
        location: str,
        columns_rename: dict = None,
        vaccine_mapping: dict = None,
        one_dose_vaccines: list = None,
    ):
        self.source_url = source_url
        self.location = location
        self.columns_rename = columns_rename
        self.vaccine_mapping = vaccine_mapping
        self.one_dose_vaccines = one_dose_vaccines
        self.vax_date_mapping = None

    def read(self):
        df = pd.read_csv(
            self.source_url,
            usecols=[
                "data_somministrazione",
                "fornitore",
                "fascia_anagrafica",
                "prima_dose",
                "seconda_dose",
                "pregressa_infezione",
                "dose_aggiuntiva",
            ],
        )
        return df

    def _check_vaccines(self, df: pd.DataFrame):
        """Get vaccine columns mapped to Vaccine names."""
        assert set(df["fornitore"].unique()) == set(self.vaccine_mapping.keys())
        return df

    def rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.rename(columns=self.columns_rename)

    def translate_vaccine_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.replace({"vaccine": self.vaccine_mapping})

    def get_total_vaccinations(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(
            total_vaccinations=df.prima_dose + df.seconda_dose + df.pregressa_infezione + df.total_boosters
        )

    def pipeline_base(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.pipe(self._check_vaccines)
            .pipe(self.rename_columns)
            .pipe(self.translate_vaccine_columns)
            .pipe(self.get_total_vaccinations)
        )

    def get_people_vaccinated(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(people_vaccinated=df["prima_dose"] + df["pregressa_infezione"])

    def get_people_fully_vaccinated(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(
            people_fully_vaccinated=lambda x: x.apply(
                lambda row: row["prima_dose"] if row["vaccine"] in self.one_dose_vaccines else row["seconda_dose"],
                axis=1,
            )
        )

    def get_final_numbers(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.groupby("date")[
                ["total_vaccinations", "people_vaccinated", "people_fully_vaccinated", "total_boosters"]
            ]
            .sum()
            .sort_index()
            .cumsum()
            .reset_index()
        )

    def enrich_location(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(location=self.location)

    def enrich_source(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(source_url=self.source_url)

    def vaccine_start_dates(self, df: pd.DataFrame):
        date2vax = sorted(
            ((df.loc[df["vaccine"] == vaccine, "date"].min(), vaccine) for vaccine in self.vaccine_mapping.values()),
            key=lambda x: x[0],
            reverse=True,
        )
        return [(date2vax[i][0], ", ".join(sorted(v[1] for v in date2vax[i:]))) for i in range(len(date2vax))]

    def enrich_vaccine(self, df: pd.DataFrame) -> pd.DataFrame:
        def _enrich_vaccine(date: str) -> str:
            for dt, vaccines in self.vax_date_mapping:
                if date >= dt:
                    return vaccines
            raise ValueError(f"Invalid date {date} in DataFrame!")

        return df.assign(vaccine=df["date"].apply(_enrich_vaccine))

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.pipe(self.get_people_vaccinated)
            .pipe(self.get_people_fully_vaccinated)
            .pipe(self.get_final_numbers)
            .pipe(self.enrich_location)
            .pipe(self.enrich_source)
            .pipe(self.enrich_vaccine)
        )

    def get_total_vaccinations_by_manufacturer(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.groupby(["date", "vaccine"])["total_vaccinations"]
            .sum()
            .sort_index()
            .reset_index()
            .assign(total_vaccinations=lambda x: x.groupby("vaccine")["total_vaccinations"].cumsum())
        )

    def pipeline_manufacturer(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.pipe(self.get_total_vaccinations_by_manufacturer).pipe(self.enrich_location)

    def to_csv(self, paths):
        vaccine_data = self.read().pipe(self.pipeline_base)

        self.vax_date_mapping = self.vaccine_start_dates(vaccine_data)

        vaccine_data.pipe(self.pipeline).to_csv(paths.tmp_vax_out(self.location), index=False)

        df_man = vaccine_data.pipe(self.pipeline_manufacturer)
        df_man.to_csv(paths.tmp_vax_out_man(self.location), index=False)
        export_metadata(
            df_man,
            "Extraordinary commissioner for the Covid-19 emergency",
            self.source_url,
            paths.tmp_vax_metadata_man,
        )


def main(paths):
    Italy(
        source_url=SOURCE_URL,
        location=LOCATION,
        columns_rename=COLUMNS_RENAME,
        vaccine_mapping=VACCINE_MAPPING,
        one_dose_vaccines=ONE_DOSE_VACCINES,
    ).to_csv(paths)


if __name__ == "__main__":
    main()
