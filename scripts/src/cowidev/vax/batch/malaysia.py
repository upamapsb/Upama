import re

import pandas as pd


class Malaysia:
    def __init__(self) -> None:
        self.location = "Malaysia"
        self.source_url = "https://github.com/CITF-Malaysia/citf-public/raw/main/vaccination/vax_malaysia.csv"
        self.source_url_ref = "https://github.com/CITF-Malaysia/citf-public"
        self.columns_rename = {
            "date": "date",
            "cumul_full": "people_fully_vaccinated",
            "cumul": "total_vaccinations",
        }
        # From https://github.com/CITF-Malaysia/citf-public/tree/main/vaccination
        self._columns_default = [
            "date",
            "daily_partial",
            "daily_full",
            "daily",
            "cumul_partial",
            "cumul_full",
            "cumul",
            "pending",
            "cumul_full_child",
            "cumul_partial_child",
            "daily_full_child",
            "daily_partial_child",
        ]
        self._vax_2d = {
            "pfizer",
            "astra",
            "sinovac",
        }
        self._vax_1d = {
            "cansino",
        }

    def read(self) -> pd.DataFrame:
        return pd.read_csv(
            self.source_url,
        )

    def pipe_rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.rename(columns=self.columns_rename)

    def _check_double_vaccines(self, df: pd.DataFrame):
        reg = r"([a-z]+)(?:1|2)"
        columns_2dose = df.filter(regex=reg).columns.tolist()
        vaccines_2dose = {re.search(reg, col).group(1) for col in columns_2dose}
        vaccines_2dose_wrong = vaccines_2dose.difference(self._vax_2d)
        if vaccines_2dose_wrong:
            raise ValueError(f"New double-vaccine(s): {vaccines_2dose_wrong}")
        return columns_2dose

    @property
    def columns_1dose(self):
        return list(self._vax_1d)

    def pipe_check_vaccines(self, df: pd.DataFrame) -> pd.DataFrame:
        # Get double-dose vaccines
        columns_2dose = self._check_double_vaccines(df)
        columns_new = df.columns.difference(self._columns_default + self.columns_1dose + columns_2dose).tolist()
        if columns_new:
            raise ValueError(
                f"New columns {columns_new}! If single-shot data, need to correct variable `people_vaccinated` in "
                "method `pipe_correct_single_shot()`"
            )
        return df

    def pipe_correct_single_shot(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(people_vaccinated=(df.cumul_partial + df[self._vax_1d].sum(axis=1)).astype(int))

    def pipe_vaccine(self, df: pd.DataFrame) -> str:
        def _enrich_vaccine(date):
            if date >= "2021-08-27":
                return "CanSino, Oxford/AstraZeneca, Pfizer/BioNTech, Sinovac"
            if date >= "2021-05-05":
                return "Oxford/AstraZeneca, Pfizer/BioNTech, Sinovac"
            if date >= "2021-03-17":
                return "Pfizer/BioNTech, Sinovac"
            return "Pfizer/BioNTech"

        return df.assign(vaccine=df.date.astype(str).apply(_enrich_vaccine))

    def pipe_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(
            location=self.location,
            source_url=self.source_url_ref,
        )

    def pipe_columns_out(self, df: pd.DataFrame) -> pd.DataFrame:
        return df[
            [
                "date",
                "people_vaccinated",
                "people_fully_vaccinated",
                "total_vaccinations",
                "vaccine",
                "location",
                "source_url",
            ]
        ]
        # return df[
        #     [
        #         "location",
        #         "date",
        #         "vaccine",
        #         "source_url",
        #         "total_vaccinations",
        #         "people_vaccinated",
        #         "people_fully_vaccinated",
        #     ]
        # ]

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.pipe(self.pipe_check_vaccines)
            .pipe(self.pipe_rename_columns)
            .pipe(self.pipe_correct_single_shot)
            .pipe(self.pipe_vaccine)
            .pipe(self.pipe_metadata)
            .pipe(self.pipe_columns_out)
        )

    def export(self, paths):
        df = self.read().pipe(self.pipeline)
        df.to_csv(paths.tmp_vax_out(self.location), index=False)


def main(paths):
    Malaysia().export(paths)
