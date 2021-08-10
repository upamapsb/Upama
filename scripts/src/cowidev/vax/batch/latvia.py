import pandas as pd

from cowidev.vax.utils.utils import get_soup
from cowidev.vax.utils.files import export_metadata


vaccine_mapping = {
    "Comirnaty": "Pfizer/BioNTech",
    "COMIRNATY": "Pfizer/BioNTech",
    "COVID-19 Vaccine Moderna": "Moderna",
    "Moderna COVID-19 vaccine": "Moderna",
    "Spikevax": "Moderna",
    "Vaxzevria": "Oxford/AstraZeneca",
    "COVID-19 Vaccine Janssen": "Johnson&Johnson",
}
one_dose_vaccines = ["Johnson&Johnson"]


class Latvia:
    def __init__(self):
        self.location = "Latvia"
        self.source_url = "https://data.gov.lv/dati/eng/dataset/covid19-vakcinacijas"

    def _get_file_link(self):
        soup = get_soup(self.source_url)
        file_url = soup.find_all("a", class_="resource-url-analytics")[-1]["href"]
        return file_url

    def read(self):
        link = self._get_file_link()
        df = pd.read_excel(
            link,
            usecols=[
                "Vakcinācijas datums",
                "Vakcinēto personu skaits",
                "Vakcinācijas posms",
                "Preparāts",
            ],
        )
        return df

    def pipe_base(self, df: pd.DataFrame) -> pd.DataFrame:
        df["vaccine"] = df["Preparāts"].str.strip()
        vaccines_wrong = set(df["vaccine"].unique()).difference(vaccine_mapping)
        if vaccines_wrong:
            raise ValueError(f"Missing vaccines: {vaccines_wrong}")
        return (
            df.replace(vaccine_mapping)
            .drop(columns=["Preparāts"])
            .replace(
                {"1.pote": "people_vaccinated", "2.pote": "people_fully_vaccinated"}
            )
        )

    def pipe_pivot(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.copy()
            .rename(columns={"Vakcinācijas datums": "date"})
            .groupby(["date", "Vakcinācijas posms", "vaccine"], as_index=False)
            .sum()
            .pivot(
                index=["date", "vaccine"],
                columns="Vakcinācijas posms",
                values="Vakcinēto personu skaits",
            )
            .reset_index()
            .sort_values("date")
        )

    def pipe_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        df["total_vaccinations"] = df["people_vaccinated"].fillna(0) + df[
            "people_fully_vaccinated"
        ].fillna(0)
        df.loc[
            df["vaccine"].isin(one_dose_vaccines), "people_fully_vaccinated"
        ] = df.people_vaccinated
        return df

    def pipe_aggregate(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.groupby("date", as_index=False).agg(
            {
                "total_vaccinations": "sum",
                "people_vaccinated": "sum",
                "people_fully_vaccinated": "sum",
                "vaccine": lambda x: ", ".join(sorted(x)),
            }
        )

    def pipe_cumsum(self, df: pd.DataFrame) -> pd.DataFrame:
        df[["total_vaccinations", "people_vaccinated", "people_fully_vaccinated"]] = df[
            ["total_vaccinations", "people_vaccinated", "people_fully_vaccinated"]
        ].cumsum()
        return df

    def pipe_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(location=self.location, source_url=self.source_url)

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.pipe(self.pipe_pivot)
            .pipe(self.pipe_metrics)
            .pipe(self.pipe_aggregate)
            .pipe(self.pipe_cumsum)
            .pipe(self.pipe_metadata)[
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
            .sort_values("date")
        )

    def pipeline_manufacturer(self, df: pd.DataFrame) -> pd.DataFrame:
        df = (
            df.rename(
                columns={
                    "Vakcinācijas datums": "date",
                    "Vakcinēto personu skaits": "total_vaccinations",
                }
            )
            .groupby(["date", "vaccine"], as_index=False)["total_vaccinations"]
            .sum()
            .sort_values("date")
        )
        df = df.assign(
            total_vaccinations=df.groupby("vaccine", as_index=False)[
                "total_vaccinations"
            ].cumsum(),
            location=self.location,
        )[["location", "date", "vaccine", "total_vaccinations"]].sort_values(
            ["date", "vaccine"]
        )
        return df

    def to_csv(self, paths):
        df = self.read()
        df_base = df.pipe(self.pipe_base)
        # Main data
        df_base.pipe(self.pipeline).to_csv(
            paths.tmp_vax_out(self.location), index=False
        )
        # Manufacturer data
        df_man = df_base.pipe(self.pipeline_manufacturer)
        df_man.to_csv(paths.tmp_vax_out_man(self.location), index=False)
        export_metadata(
            df_man,
            "National Health Service",
            self.source_url,
            paths.tmp_vax_metadata_man,
        )


def main(paths):
    Latvia().to_csv(paths)


if __name__ == "__main__":
    main()
