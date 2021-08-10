import requests
from functools import reduce
import pandas as pd

from cowidev.vax.utils.dates import clean_date_series


class Greece:
    def __init__(self, source_url: str, source_url_ref: str, location: str):
        """Constructor.

        Args:
            source_url (str): Source data url
            source_url_ref (str): Source data reference url
            location (str): Location name
        """
        self.source_url = source_url
        self.source_url_ref = source_url_ref
        self.location = location

    def read(self) -> pd.DataFrame:
        data = requests.get(self.source_url).json()
        return self.parse_data(data)

    def parse_data(self, data: dict):
        metrics_mapping = {
            "Συνολικοί εμβολιασμοί με τουλάχιστον 1 δόση": "people_partly_vaccinated",
            "Συνολικοί ολοκληρωμένοι εμβολιασμοί": "people_fully_vaccinated",
            "Συνολικοί εμβολιασμοί": "total_vaccinations",
            "Σύνολο ατόμων που έχουν εμβολιαστεί ": "people_vaccinated",
        }
        dfs = [
            pd.DataFrame.from_records(d["data"]).rename(
                columns={
                    "x": "date",
                    "y": metrics_mapping[d["label"]],
                }
            )
            for d in data
        ]
        return reduce(
            lambda left, right: pd.merge(left, right, on=["date"], how="inner"), dfs
        )

    def pipe_replace_nulls_with_nans(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(
            people_fully_vaccinated=df.people_fully_vaccinated.replace(0, pd.NA)
        )

    def pipe_date(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(date=clean_date_series(df.date, "%Y-%m-%dT%H:%M:%S"))

    def pipe_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(
            location=self.location,
            source_url=self.source_url_ref,
        )

    def pipe_vaccine(self, df: pd.DataFrame) -> pd.DataFrame:
        def _enrich_vaccine_name(date: str) -> str:
            if date < "2021-01-13":
                return "Pfizer/BioNTech"
            elif "2021-01-13" <= date < "2021-02-10":
                return "Moderna, Pfizer/BioNTech"
            elif "2021-02-10" <= date < "2021-04-28":
                return "Moderna, Oxford/AstraZeneca, Pfizer/BioNTech"
            elif "2021-04-28" <= date:
                return "Johnson&Johnson, Moderna, Oxford/AstraZeneca, Pfizer/BioNTech"

        return df.assign(vaccine=df.date.apply(_enrich_vaccine_name))

    def pipe_select_output_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return df[
            [
                "date",
                "location",
                "vaccine",
                "source_url",
                "total_vaccinations",
                "people_vaccinated",
                "people_fully_vaccinated",
            ]
        ]

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.pipe(self.pipe_replace_nulls_with_nans)
            .pipe(self.pipe_date)
            .pipe(self.pipe_metadata)
            .pipe(self.pipe_vaccine)
            .pipe(self.pipe_select_output_columns)
        )

    def to_csv(self, paths):
        """Generalized."""
        df = self.read().pipe(self.pipeline)
        df.to_csv(paths.tmp_vax_out(self.location), index=False)


def main(paths):
    Greece(
        source_url="https://www.data.gov.gr/api/v1/summary/mdg_emvolio?date_from=2020-12-28",
        source_url_ref="https://www.data.gov.gr/datasets/mdg_emvolio/",
        location="Greece",
    ).to_csv(paths)


if __name__ == "__main__":
    main()
