import pandas as pd

from cowidev.utils.web import request_json
from cowidev.vax.utils.utils import make_monotonic


class Canada:
    location: str = "Canada"
    source_url: str = "https://api.covid19tracker.ca/reports"
    source_url_ref: str = "https://covid19tracker.ca/vaccinationtracker.html"
    source_url_boosters: str = "https://api.covid19tracker.ca/vaccines/reports/latest"

    def read(self) -> pd.DataFrame:
        data = request_json(self.source_url)
        return pd.DataFrame.from_records(data["data"], columns=["date", "total_vaccinations", "total_vaccinated"])

    def _get_boosters_data(self):
        data = request_json(self.source_url_boosters)
        df = pd.DataFrame.from_records(data["data"])
        total_boosters = df.total_boosters_1.sum().astype(int)
        total_boosters_date = df.date.max()
        return total_boosters_date, total_boosters

    def pipe_filter_rows(self, df: pd.DataFrame):
        # Only records since vaccination campaign started
        return df[df.total_vaccinations > 0]

    def pipe_rename_columns(self, df: pd.DataFrame):
        return df.rename(columns={"total_vaccinated": "people_fully_vaccinated"})

    def pipe_people_vaccinated(self, df: pd.DataFrame):
        df = df.assign(
            people_vaccinated=(
                df.total_vaccinations - df.people_fully_vaccinated - df.total_boosters.fillna(method="ffill").fillna(0)
            )
        )
        # Booster data was not recorded for this dates, hence estimations on people vaccinated will not be accurate
        df.loc[(df.date >= "2021-10-04") & (df.date <= "2021-10-09"), "people_vaccinated"] = pd.NA
        return df

    def pipe_metadata(self, df: pd.DataFrame):
        return df.assign(
            location=self.location,
            source_url=self.source_url_ref,
            vaccine="Moderna, Oxford/AstraZeneca, Pfizer/BioNTech",
        )

    def pipe_total_boosters(self, df: pd.DataFrame):
        dt, v = self._get_boosters_data()
        df.loc[df.date == dt, "total_boosters"] = v
        return df

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        df = (
            df.pipe(self.pipe_filter_rows)
            .pipe(self.pipe_rename_columns)
            .pipe(self.pipe_total_boosters)
            .pipe(self.pipe_people_vaccinated)
            .pipe(self.pipe_metadata)
            .pipe(self.pipe_total_boosters)
            .pipe(make_monotonic)
            .sort_values("date")
        )
        return df

    def export(self, paths):
        destination = paths.tmp_vax_out(self.location)
        self.read().pipe(self.pipeline).to_csv(destination, index=False)


def main(paths):
    Canada().export(paths)
