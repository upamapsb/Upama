import pandas as pd

from cowidev.vax.utils.incremental import enrich_data, increment


class Brazil:
    def __init__(self) -> None:
        self.location = "Brazil"
        self.source_url = "https://raw.githubusercontent.com/wcota/covid19br/master/cases-brazil-total.csv"
        self.source_url_ref = "https://coronavirusbra1.github.io"

    def read(self):
        df = pd.read_csv(self.source_url)
        df = df[df.state == "TOTAL"].iloc[0]
        return pd.Series(
            {
                "date": df.date,
                "total_vaccinations": df.vaccinated
                + df.vaccinated_second
                + df.vaccinated_single
                + df.vaccinated_third,
                "people_vaccinated": df.vaccinated + df.vaccinated_single,
                "people_fully_vaccinated": df.vaccinated_second + df.vaccinated_single,
                "total_boosters": df.vaccinated_third,
            }
        )

    def pipe_location(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "location", self.location)

    def pipe_source(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "source_url", self.source_url_ref)

    def pipe_vaccine(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "vaccine", "Johnson&Johnson, Pfizer/BioNTech, Oxford/AstraZeneca, Sinovac")

    def pipeline(self, ds: pd.Series) -> pd.Series:
        return ds.pipe(self.pipe_location).pipe(self.pipe_source).pipe(self.pipe_vaccine)

    def export(self):
        data = self.read().pipe(self.pipeline)
        increment(
            location=data["location"],
            total_vaccinations=data["total_vaccinations"],
            people_vaccinated=data["people_vaccinated"],
            people_fully_vaccinated=data["people_fully_vaccinated"],
            total_boosters=data["total_boosters"],
            date=data["date"],
            source_url=data["source_url"],
            vaccine=data["vaccine"],
        )


def main():
    Brazil().export()
