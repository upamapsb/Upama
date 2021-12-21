import pandas as pd

from cowidev.utils.web import request_json
from cowidev.vax.utils.files import export_metadata_manufacturer
from cowidev.utils import paths


class HongKong:
    location: str = "Hong Kong"
    source_url: str = "https://static.data.gov.hk/covid-vaccine/bar_vaccination_date.json"
    source_url_ref: str = "https://www.covidvaccine.gov.hk/en/dashboard"

    def read(self) -> pd.DataFrame:
        data = request_json(self.source_url)
        return pd.DataFrame.from_dict(
            [
                {
                    "date": d["date"],
                    "people_vaccinated": d["firstDose"]["cumulative"]["total"],
                    "people_fully_vaccinated": d["secondDose"]["cumulative"]["total"],
                    "total_vaccinations": d["totalDose"]["cumulative"]["total"],
                    "total_boosters": d["thirdDose"]["cumulative"]["total"],
                    "total_pfizer": d["totalDose"]["cumulative"]["biontech"],
                    "total_sinovac": d["totalDose"]["cumulative"]["sinovac"],
                }
                for d in data
            ]
        )

    def pipe_vaccine(self, df: pd.DataFrame) -> pd.DataFrame:
        def _enrich_vaccine(date: str) -> str:
            if date < "2021-03-06":
                return "Sinovac"
            return "Pfizer/BioNTech, Sinovac"

        return df.assign(vaccine=df.date.apply(_enrich_vaccine))

    def pipe_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(location=self.location, source_url=self.source_url_ref)

    def pipeline_base(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.pipe(self.pipe_vaccine).pipe(self.pipe_metadata)

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.drop(columns=["total_pfizer", "total_sinovac"])[
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

    def pipeline_manufacturer(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df[["location", "date", "total_pfizer", "total_sinovac"]]
            .rename(columns={"total_pfizer": "Pfizer/BioNTech", "total_sinovac": "Sinovac"})
            .set_index(["location", "date"])
            .stack()
            .reset_index()
            .rename(columns={"level_2": "vaccine", 0: "total_vaccinations"})
        )

    def export(self):
        df_base = self.read().pipe(self.pipeline_base)

        # Main data
        destination = paths.out_vax(self.location)
        df = df_base.pipe(self.pipeline)
        df.to_csv(destination, index=False)
        # Manufacturer
        destination = paths.out_vax(self.location, manufacturer=True)
        df_manuf = df_base.pipe(self.pipeline_manufacturer)
        df_manuf.to_csv(destination, index=False)
        export_metadata_manufacturer(df_manuf, "Government of Hong Kong", self.source_url_ref)


def main():
    HongKong().export()


if __name__ == "__main__":
    main()
