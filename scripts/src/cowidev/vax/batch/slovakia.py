import pandas as pd

from cowidev.utils import paths
from cowidev.utils.utils import check_known_columns
from cowidev.vax.utils.checks import VACCINES_ONE_DOSE
from cowidev.vax.utils.utils import build_vaccine_timeline


class Slovakia:
    location: str = "Slovakia"
    source_url: str = (
        "https://github.com/Institut-Zdravotnych-Analyz/covid19-data/raw/main/Vaccination/"
        "OpenData_Slovakia_Vaccination_Regions.csv"
    )
    source_url_ref: str = "https://github.com/Institut-Zdravotnych-Analyz/covid19-data"
    vaccine_rename: dict = {
        "Comirnaty koncentrát na injekčnú disperziu": "Pfizer/BioNTech",
        "Comirnaty 10 mikrogramov/dávka koncentrát na injekčnú disperziu, con dsi 10x1,3 ml (liek.inj.skl.)": (
            "Pfizer/BioNTech"
        ),
        "Spikevax injekčná disperzia (pôvodne COVID-19 Vaccine Moderna)dis inj 10x5 ml (liek.inj.skl.)": "Moderna",
        "Vaxzevria injekčná suspenzia sus inj 10x5 ml (liek.inj.skl.) (pôvodne COVID-19 Vaccine AstraZeneca)": (
            "Oxford/AstraZeneca"
        ),
        "COVID-19 Vaccine Janssen injekčná suspenzia sus inj 10x2,5 ml (liek.inj.skl.)": "Johnson&Johnson",
        "COVID-19 Vaccine Janssen injekčná suspenzia sus inj 20x2,5 ml (liek.inj.skl.)": "Johnson&Johnson",
        "Sputnik V(Gam-COVID-Vac) lag orig. 5x3 ml, komponenta I-rekombinantný ľudský adenovírus sérotypu 26,komponenta I-rekombinantný ľ": (
            "Sputnik V"
        ),
        "Vaxzevria injekčná suspenzia sus inj 10x4 ml (liek.inj.skl.) (pôvodne COVID-19 Vaccine AstraZeneca)": (
            "Oxford/AstraZeneca"
        ),
    }
    column_rename: dict = {"Date": "date"}

    def read(self):
        df = pd.read_csv(self.source_url, sep=";")
        check_known_columns(
            df, ["Date", "Region", "Region_code", "Vaccine_name", "first_dose", "second_dose", "third_dose"]
        )
        return df

    def pipe_vaccine_rename(self, df: pd.DataFrame) -> pd.DataFrame:
        vax_wrong = set(df["Vaccine_name"]).difference(self.vaccine_rename.keys())
        if vax_wrong:
            raise ValueError(
                f"Unknown vaccines detected {vax_wrong}! Please,  review class attribute self.vaccine_rename."
            )
        df = df.assign(vaccine=df["Vaccine_name"].map(self.vaccine_rename))
        return df

    def pipe_rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.rename(columns=self.column_rename)

    def pipe_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        single_dose = df.first_dose.copy()
        single_dose.loc[~df.vaccine.isin(VACCINES_ONE_DOSE)] = 0
        df = df.assign(
            total_vaccinations=df.first_dose + df.second_dose + df.third_dose,
            people_vaccinated=df.first_dose,
            people_fully_vaccinated=df.second_dose + single_dose,
            total_boosters=df.third_dose,
        )
        return df

    def pipe_metrics_aggregate(self, df: pd.DataFrame) -> pd.DataFrame:
        vax_timeline = df.groupby("vaccine").date.min().to_dict()
        df = (
            df.groupby("date", as_index=False)
            .agg(
                {
                    "total_vaccinations": "sum",
                    "people_vaccinated": "sum",
                    "people_fully_vaccinated": "sum",
                    "total_boosters": "sum",
                }
            )
            .sort_values("date")  # change to descending
        )
        # Add vaccines
        return build_vaccine_timeline(
            df,
            vax_timeline,
        )

    def pipe_metrics_cumsum(self, df: pd.DataFrame) -> pd.DataFrame:
        df["people_vaccinated"] = df["people_vaccinated"].cumsum()
        df["people_fully_vaccinated"] = df["people_fully_vaccinated"].cumsum()
        df["total_boosters"] = df["total_boosters"].cumsum()
        df["total_vaccinations"] = df["total_vaccinations"].cumsum()
        return df

    def pipe_vaccine(self, df: pd.DataFrame) -> pd.DataFrame:
        df.loc[:, "vaccine"] = "Pfizer/BioNTech"
        df.loc[df.date >= "2021-01-27", "vaccine"] = "Moderna, Pfizer/BioNTech"
        df.loc[df.date >= "2021-02-13", "vaccine"] = "Moderna, Oxford/AstraZeneca, Pfizer/BioNTech, Sputnik V"
        return df

    def pipe_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.assign(location=self.location, source_url=self.source_url_ref)
        return df

    def pipe_out_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return df[
            [
                "location",
                "date",
                "vaccine",
                "source_url",
                "total_vaccinations",
                "people_vaccinated",
                "people_fully_vaccinated",
                "total_boosters",
            ]
        ]

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.pipe(self.pipe_vaccine_rename)
            .pipe(self.pipe_rename_columns)
            .pipe(self.pipe_metrics)
            .pipe(self.pipe_metrics_aggregate)
            .pipe(self.pipe_metrics_cumsum)
            # .pipe(self.pipe_vaccine)
            .pipe(self.pipe_metadata)
            .pipe(self.pipe_out_columns)
        )

    def export(self):
        df = self.read().pipe(self.pipeline)
        df.to_csv(paths.out_vax(self.location), index=False)


def main():
    Slovakia().export()


if __name__ == "__main__":
    main()
