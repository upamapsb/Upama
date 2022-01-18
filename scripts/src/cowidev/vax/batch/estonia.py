import pandas as pd

from cowidev.utils import paths
from cowidev.utils.utils import check_known_columns
from cowidev.vax.utils.utils import build_vaccine_timeline


class Estonia:
    location: str = "Estonia"
    # We should soon migrate to v3 of the API. Currently waiting for documentation for v3 to be released at
    # https://www.terviseamet.ee/et/koroonaviirus/avaandmed
    source_url: str = "https://opendata.digilugu.ee/covid19/vaccination/v2/opendata_covid19_vaccination_total.json"
    source_url_ref: str = "https://opendata.digilugu.ee"

    def read(self) -> pd.DataFrame:
        return self._parse_data()

    def _parse_data(self) -> pd.DataFrame:
        df = pd.read_json(self.source_url)

        check_known_columns(
            df,
            [
                "StatisticsDate",
                "TargetDiseaseCode",
                "TargetDisease",
                "MeasurementType",
                "DailyCount",
                "TotalCount",
                "PopulationCoverage",
            ],
        )
        assert set(df.MeasurementType) == {
            "Vaccinated",
            "DosesAdministered",
            "FullyVaccinated",
        }, "New MeasurementType found!"

        df = (
            df[["StatisticsDate", "MeasurementType", "TotalCount"]]
            .pivot(index="StatisticsDate", columns="MeasurementType", values="TotalCount")
            .reset_index()
            .rename(
                columns={
                    "StatisticsDate": "date",
                    "DosesAdministered": "total_vaccinations",
                    "FullyVaccinated": "people_fully_vaccinated",
                    "Vaccinated": "people_vaccinated",
                }
            )
        )
        return df[["date", "people_fully_vaccinated", "people_vaccinated", "total_vaccinations"]]

    def pipe_location(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(location=self.location)

    def pipe_vaccine_name(self, df: pd.DataFrame) -> pd.DataFrame:
        df = build_vaccine_timeline(
            df,
            {
                "Pfizer/BioNTech": "2020-12-01",
                "Moderna": "2021-01-14",
                "Oxford/AstraZeneca": "2021-02-09",
                "Johnson&Johnson": "2021-04-14",
            },
        )
        return df

    def pipe_source(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(source_url=self.source_url_ref)

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.pipe(self.pipe_location).pipe(self.pipe_vaccine_name).pipe(self.pipe_source)

    def export(self):
        destination = paths.out_vax(self.location)
        self.read().pipe(self.pipeline).to_csv(destination, index=False)


def main():
    Estonia().export()


if __name__ == "__main__":
    main()
