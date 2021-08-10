import pandas as pd


class Malaysia:
    def __init__(self) -> None:
        self.location = "Malaysia"
        self.source_url = "https://github.com/CITF-Malaysia/citf-public/raw/main/vaccination/vax_malaysia.csv"
        self.source_url_ref = "https://github.com/CITF-Malaysia/citf-public"

    def read(self) -> pd.DataFrame:
        return pd.read_csv(
            self.source_url,
            usecols=["date", "dose1_cumul", "dose2_cumul", "total_cumul"],
        )

    def pipe_rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.rename(
            columns={
                "dose1_cumul": "people_vaccinated",
                "dose2_cumul": "people_fully_vaccinated",
                "total_cumul": "total_vaccinations",
            }
        )

    def pipe_vaccine(self, df: pd.DataFrame) -> str:
        def _enrich_vaccine(date):
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

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.pipe(self.pipe_rename_columns)
            .pipe(self.pipe_vaccine)
            .pipe(self.pipe_metadata)
        )

    def export(self, paths):
        df = self.read().pipe(self.pipeline)
        df.to_csv(paths.tmp_vax_out(self.location), index=False)


def main(paths):
    Malaysia().export(paths)
