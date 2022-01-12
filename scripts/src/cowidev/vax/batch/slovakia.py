from datetime import datetime, timedelta

import pandas as pd

from cowidev.utils import clean_date, paths
from cowidev.utils.clean.dates import localdatenow
from cowidev.utils.utils import check_known_columns
from cowidev.vax.utils.checks import VACCINES_ONE_DOSE
from cowidev.vax.utils.utils import build_vaccine_timeline


class Slovakia:
    location = "Slovakia"
    source_url = (
        "https://github.com/Institut-Zdravotnych-Analyz/covid19-data/raw/main/Vaccination/"
        "OpenData_Slovakia_Vaccination_AgeGroup_District.csv"
    )
    source_url_ref = "https://github.com/Institut-Zdravotnych-Analyz/covid19-data"
    vaccine_mapping = {
        "ASTRAZENECA": "Oxford/AstraZeneca",
        "COMIRNATY": "Pfizer/BioNTech",
        "JANSSEN": "Johnson&Johnson",
        "MODERNA": "Moderna",
        "SPUTNIK": "Sputnik V",
    }
    vax_timeline = None
    date_start = datetime(2021, 1, 4)

    def read(self):
        df = pd.read_csv(self.source_url, sep=";")
        check_known_columns(
            df,
            [
                "week",
                "vaccine",
                "gender",
                "AgeGroup",
                "region",
                "district",
                "district_code",
                "dose",
                "doses_administered",
            ],
        )
        return df

    def pipe_date(self, df: pd.DataFrame) -> pd.DataFrame:
        # Change week to date

        return df.assign(date=df.week.apply(self._week_to_date))

    def _week_to_date(self, week):
        date = clean_date(self.date_start + timedelta(weeks=week))
        if date > localdatenow("Europe/Bratislava"):
            return localdatenow("Europe/Bratislava")
        return date

    def pipe_vaccine_checks(self, df: pd.DataFrame) -> pd.DataFrame:
        # Get vax timeline
        vax_wrong = set(df.vaccine).difference(self.vaccine_mapping)
        if vax_wrong:
            raise ValueError(f"Unknown vaccine(s): {vax_wrong}")
        df = df.assign(vaccine=df.vaccine.replace(self.vaccine_mapping))
        self.vax_timeline = df.groupby("vaccine").date.min().to_dict()
        # Check one dose vax
        vax_1d = [vax for vax in self.vaccine_mapping.values() if vax in VACCINES_ONE_DOSE]
        if "2" in set(df.loc[df.vaccine.isin(vax_1d), "dose"]):
            raise ValueError("Some single-dose vaccines are registering second doses!")
        return df

    def pipe_reshape(self, df: pd.DataFrame) -> pd.DataFrame:
        # Group
        df = df.groupby(["date", "dose"], as_index=False).doses_administered.sum()
        # Pivot
        df = df.pivot(index=["date"], columns="dose", values="doses_administered").reset_index()
        return df

    def pipe_cumsum(self, df: pd.DataFrame) -> pd.DataFrame:
        # Cummulative
        df = df.sort_values("date")
        cols = ["1", "2", "fully", "3"]
        df[cols] = df[cols].cumsum().fillna(method="ffill").fillna(0)
        return df

    def pipe_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        # Metrics
        return df.assign(
            total_vaccinations=df["1"] + df["2"] + df["3"],
            people_vaccinated=df["1"],
            people_fully_vaccinated=df.fully,
            total_boosters=df["3"],
        )

    def pipe_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        # Metadata
        df = df.assign(location=self.location, source_url=self.source_url)
        # Add vaccines
        return build_vaccine_timeline(
            df,
            self.vax_timeline,
        )

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
            df.pipe(self.pipe_date)
            .pipe(self.pipe_vaccine_checks)
            .pipe(self.pipe_reshape)
            .pipe(self.pipe_cumsum)
            .pipe(self.pipe_metrics)
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
