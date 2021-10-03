import pandas as pd

from cowidev.vax.utils.utils import make_monotonic
from cowidev.utils.clean import clean_date


class Australia:
    def __init__(self):
        """Constructor.

        Args:
            source_url (str): Source data url
            location (str): Location name
            columns_rename (dict, optional): Maps original to new names. Defaults to None.
        """
        self.source_url = "https://covidlive.com.au/covid-live.json"
        self.location = "Australia"
        self.columns_rename = {
            "REPORT_DATE": "date",
            "VACC_DOSE_CNT": "total_vaccinations",
            "VACC_PEOPLE_CNT": "people_fully_vaccinated",
        }

    def read(self) -> pd.DataFrame:
        return pd.read_json(self.source_url)

    def pipe_filter_rows(self, df: pd.DataFrame) -> pd.DataFrame:
        return df[(df.NAME == self.location) & df.VACC_DOSE_CNT.notnull()]

    def pipe_rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return df[self.columns_rename.keys()].rename(columns=self.columns_rename)

    def pipe_people_full_vaccinated(self, df: pd.DataFrame) -> pd.DataFrame:
        date_limit_low = "2021-03-14"
        date_limit_up = "2021-05-23"
        df.loc[
            (date_limit_low <= df.date) & (df.date < date_limit_up),
            "people_fully_vaccinated",
        ] = pd.NA
        df.loc[df.date < date_limit_low, "people_fully_vaccinated"] = 0
        return df

    def pipe_people_vaccinated(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.assign(
            people_vaccinated=df.total_vaccinations - df.people_fully_vaccinated,
        )
        return df

    def pipe_date(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(date=df.date.apply(clean_date, fmt="%Y-%m-%d", minus_days=1))

    def pipe_vaccine(self, df: pd.DataFrame) -> pd.DataFrame:
        def _enrich_vaccine(date: str) -> str:
            if date >= "2021-03-07":
                return "Moderna, Oxford/AstraZeneca, Pfizer/BioNTech"
            return "Pfizer/BioNTech"

        return df.assign(vaccine=df.date.astype(str).apply(_enrich_vaccine))

    def pipe_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(location=self.location, source_url=self.source_url)

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.pipe(self.pipe_filter_rows)
            .pipe(self.pipe_rename_columns)
            .pipe(self.pipe_people_full_vaccinated)
            .pipe(self.pipe_people_vaccinated)
            .pipe(self.pipe_vaccine)
            .pipe(self.pipe_date)
            .pipe(self.pipe_metadata)
            .pipe(make_monotonic)
            .sort_values("date")
        )

    def to_csv(self, paths):
        """Generalized."""
        df = self.read().pipe(self.pipeline)
        df.to_csv(paths.tmp_vax_out(self.location), index=False)


def main(paths):
    Australia().to_csv(paths)
