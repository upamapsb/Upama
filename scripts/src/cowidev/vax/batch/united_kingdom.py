import locale

import pandas as pd

from cowidev.vax.utils.utils import make_monotonic
from uk_covid19 import Cov19API


class UnitedKingdom:
    def __init__(self) -> None:
        self.location = "United Kingdom"
        self.source_url = "https://coronavirus.data.gov.uk/details/vaccinations"

    def read(self):
        dfs = [
            self._read_metrics("areaType=overview"),
            self._read_metrics("areaType=nation"),
        ]
        df = pd.concat(dfs).reset_index(drop=True)
        return df

    def _read_metrics(self, filters):
        metrics = {
            "date": "date",
            "location": "areaName",
            "areaCode": "areaCode",
            "people_vaccinated_report": "cumPeopleVaccinatedFirstDoseByPublishDate",
            "people_vaccinated": "cumPeopleVaccinatedFirstDoseByVaccinationDate",
            "people_fully_vaccinated_report": "cumPeopleVaccinatedCompleteByPublishDate",
            "people_fully_vaccinated": "cumPeopleVaccinatedCompleteByVaccinationDate",
            "total_vaccinations": "cumVaccinesGivenByPublishDate",
            "vaccinations_age": "vaccinationsAgeDemographics",
        }
        api = Cov19API(
            filters=[filters],
            structure=metrics,
        )
        df = api.get_dataframe()
        return df

    def _fix_metric(self, df: pd.DataFrame, metric: str) -> pd.DataFrame:
        return df.assign(**{metric: df[f"{metric}_report"].fillna(df[metric])})

    def pipe_fix_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.pipe(self._fix_metric, "people_vaccinated").pipe(self._fix_metric, "people_fully_vaccinated")
        cols = ["people_vaccinated", "people_fully_vaccinated", "total_vaccinations"]
        df = df.sort_values(["location", "date"])
        _tmp = df.groupby("location", as_index=False)[cols].fillna(method="ffill").fillna(0)
        df.loc[_tmp.index, cols] = _tmp
        df = df.assign(total_vaccinations=df[["total_vaccinations", "people_vaccinated"]].max(axis=1))
        return df

    def pipe_aggregate_first_date(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.groupby(
                [
                    "location",
                    "total_vaccinations",
                    "people_vaccinated",
                    "people_fully_vaccinated",
                ],
                as_index=False,
                dropna=False,
            )[["date"]]
            .min()
            .replace(0, pd.NA)
        )

    def pipe_source_url(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(source_url=self.source_url)

    def pipe_vaccine(self, df: pd.DataFrame) -> pd.DataFrame:
        def _enrich_vaccine(date: str) -> str:
            if date < "2021-01-04":
                return "Pfizer/BioNTech"
            elif "2021-04-07" > date >= "2021-01-04":
                return "Oxford/AstraZeneca, Pfizer/BioNTech"
            elif date >= "2021-04-07":
                # https://www.reuters.com/article/us-health-coronavirus-britain-moderna-idUSKBN2BU0KG
                return "Moderna, Oxford/AstraZeneca, Pfizer/BioNTech"

        return df.assign(vaccine=df.date.apply(_enrich_vaccine))

    def pipe_select_output_cols(self, df: pd.DataFrame) -> pd.DataFrame:
        return df[
            [
                "location",
                "date",
                "vaccine",
                "source_url",
                "total_vaccinations",
                "people_vaccinated",
                "people_fully_vaccinated",
            ]
        ]

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.pipe(self.pipe_fix_metrics)
            .pipe(self.pipe_aggregate_first_date)
            .pipe(self.pipe_source_url)
            .pipe(self.pipe_vaccine)
            .pipe(self.pipe_select_output_cols)
            .sort_values(by=["location", "date"])
        )

    def _filter_location(self, df: pd.DataFrame, location: str) -> pd.DataFrame:
        return df[df.location == location].assign(location=location)

    def to_csv(self, paths):
        df = self.read().pipe(self.pipeline)
        for location in set(df.location):
            df.pipe(self._filter_location, location).pipe(make_monotonic).to_csv(
                paths.tmp_vax_out(location), index=False
            )


def main(paths):
    locale.setlocale(locale.LC_ALL, "en_GB")
    UnitedKingdom().to_csv(paths)
