import pandas as pd
from cowidev.utils import paths


class Estonia:
    location: str = "Estonia"
    source_url: str = "https://opendata.digilugu.ee/covid19/vaccination/v2/opendata_covid19_vaccination_total.json"
    source_url_ref: str = "https://opendata.digilugu.ee"

    def read(self) -> pd.DataFrame:
        return self._parse_data()

    def _parse_data(self) -> pd.DataFrame:
        df = pd.read_json(self.source_url)
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
        def _enrich_vaccine_name(date: str) -> str:
            if date < "2021-01-14":
                return "Pfizer/BioNTech"
            elif "2021-01-14" <= date < "2021-02-09":
                return "Moderna, Pfizer/BioNTech"
            elif "2021-02-09" <= date < "2021-04-14":
                return "Moderna, Oxford/AstraZeneca, Pfizer/BioNTech"
            elif "2021-04-14" <= date:
                # https://vaktsineeri.ee/covid-19/vaktsineerimine-eestis/
                # https://vaktsineeri.ee/uudised/sel-nadalal-alustatakse-lamavate-haigete-ja-liikumisraskustega-inimeste-kodus-vaktsineerimist/
                return "Johnson&Johnson, Moderna, Oxford/AstraZeneca, Pfizer/BioNTech"

        return df.assign(vaccine=df.date.apply(_enrich_vaccine_name))

    def pipe_source(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(source_url="https://opendata.digilugu.ee")

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.pipe(self.pipe_location).pipe(self.pipe_vaccine_name).pipe(self.pipe_source)

    def export(self):
        destination = paths.out_vax(self.location)
        self.read().pipe(self.pipeline).to_csv(destination, index=False)


def main():
    Estonia().export()


if __name__ == "__main__":
    main()
