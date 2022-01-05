import pandas as pd

from cowidev.utils.web import request_json
from cowidev.utils import paths
from cowidev.utils.utils import check_known_columns
from cowidev.vax.utils.utils import make_monotonic


class Canada:
    location: str = "Canada"
    source_url: str = "https://api.covid19tracker.ca/reports"
    source_url_ref: str = "https://covid19tracker.ca/vaccinationtracker.html"
    source_url_boosters: str = "https://api.covid19tracker.ca/vaccines/reports/latest"
    df_boosters: pd.DataFrame = None

    def read(self) -> pd.DataFrame:
        data = request_json(self.source_url)
        df = pd.DataFrame.from_records(data["data"])
        check_known_columns(
            df,
            [
                "date",
                "change_cases",
                "change_fatalities",
                "change_tests",
                "change_hospitalizations",
                "change_criticals",
                "change_recoveries",
                "change_vaccinations",
                "change_vaccinated",
                "change_boosters_1",
                "change_vaccines_distributed",
                "total_cases",
                "total_fatalities",
                "total_tests",
                "total_hospitalizations",
                "total_criticals",
                "total_recoveries",
                "total_vaccinations",
                "total_vaccinated",
                "total_boosters_1",
                "total_vaccines_distributed",
            ],
        )
        return df[["date", "total_vaccinations", "total_vaccinated", "total_boosters_1"]]

    def pipe_filter_rows(self, df: pd.DataFrame):
        # Only records since vaccination campaign started
        return df[df.total_vaccinations > 0]

    def pipe_rename_columns(self, df: pd.DataFrame):
        return df.rename(columns={"total_vaccinated": "people_fully_vaccinated", "total_boosters_1": "total_boosters"})

    def pipe_people_vaccinated(self, df: pd.DataFrame):
        df = df.assign(people_vaccinated=(df.total_vaccinations - df.people_fully_vaccinated - df.total_boosters))
        # Booster data was not recorded for these dates, hence estimations on people vaccinated will not be accurate
        # df.loc[(df.date >= "2021-10-04") & (df.date <= "2021-10-09"), "people_vaccinated"] = pd.NA
        return df

    def pipe_metadata(self, df: pd.DataFrame):
        return df.assign(
            location=self.location,
            source_url=self.source_url_ref,
            vaccine="Moderna, Oxford/AstraZeneca, Pfizer/BioNTech",
        )

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        df = (
            df.pipe(self.pipe_filter_rows)
            .pipe(self.pipe_rename_columns)
            .pipe(self.pipe_people_vaccinated)
            .pipe(self.pipe_metadata)
            .pipe(make_monotonic)
            .sort_values("date")
        )
        return df

    def export(self):
        destination = paths.out_vax(self.location)
        self.read().pipe(self.pipeline).to_csv(destination, index=False)


def main():
    Canada().export()
