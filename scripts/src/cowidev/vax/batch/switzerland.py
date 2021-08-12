import requests

import pandas as pd

from cowidev.vax.utils.files import export_metadata


class Switzerland:
    def __init__(self):
        self.source_url = "https://opendata.swiss/en/dataset/covid-19-schweiz"

    def read(self):
        doses_url, people_url, manufacturer_url = self._get_file_url()
        df, df_manufacturer = self._parse_data(doses_url, people_url, manufacturer_url)
        return df, df_manufacturer

    def _get_file_url(self) -> str:
        response = requests.get("https://www.covid19.admin.ch/api/data/context").json()
        context = response["sources"]["individual"]["csv"]
        doses_url = context["vaccDosesAdministered"]
        people_url = context["vaccPersonsV2"]
        manufacturer_url = context["weeklyVacc"]["byVaccine"]["vaccDosesAdministered"]
        return doses_url, people_url, manufacturer_url

    def _parse_data(self, doses_url, people_url, manufacturer_url):
        doses = pd.read_csv(
            doses_url,
            usecols=["geoRegion", "date", "sumTotal", "type"],
        )
        people = pd.read_csv(
            people_url,
            usecols=["geoRegion", "date", "sumTotal", "type"],
        )
        manufacturer = pd.read_csv(
            manufacturer_url,
            usecols=["date", "geoRegion", "vaccine", "sumTotal"],
        )
        return pd.concat([doses, people], ignore_index=True), manufacturer

    def pipe_filter_country(self, df: pd.DataFrame, country_code: str) -> pd.DataFrame:
        return df[df.geoRegion == country_code]

    def pipe_pivot(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.pivot(index=["geoRegion", "date"], columns="type", values="sumTotal")
            .reset_index()
            .sort_values("date")
        )

    def pipe_rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.rename(
            columns={
                "geoRegion": "location",
                "COVID19FullyVaccPersons": "people_fully_vaccinated",
                "COVID19VaccDosesAdministered": "total_vaccinations",
                "COVID19AtLeastOneDosePersons": "people_vaccinated",
            }
        )

    def pipe_fix_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        df.loc[
            df.total_vaccinations < df.people_vaccinated, "total_vaccinations"
        ] = df.people_vaccinated
        return df

    def pipe_translate_country_code(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(
            location=df.location.replace({"CH": "Switzerland", "FL": "Liechtenstein"})
        )

    def pipe_source(self, df: pd.DataFrame, country_code: str) -> pd.DataFrame:
        return df.assign(
            source_url=f"{self.source_url}?detGeo={country_code}",
        )

    def pipe_vaccine(self, df: pd.DataFrame) -> pd.DataFrame:
        def _enrich_vaccine(date: str) -> str:
            if date >= "2021-01-29":
                return "Moderna, Pfizer/BioNTech"
            return "Pfizer/BioNTech"

        return df.assign(vaccine=df.date.astype(str).apply(_enrich_vaccine))

    def pipeline(self, df: pd.DataFrame, country_code: str) -> pd.DataFrame:
        return (
            df.pipe(self.pipe_filter_country, country_code)
            .pipe(self.pipe_pivot)
            .pipe(self.pipe_rename_columns)
            .pipe(self.pipe_fix_metrics)
            .pipe(self.pipe_translate_country_code)
            .pipe(self.pipe_source, country_code)
            .pipe(self.pipe_vaccine)[
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
        )

    def pipeline_manufacturer(self, df: pd.DataFrame) -> pd.DataFrame:
        vaccine_mapping = {
            "pfizer_biontech": "Pfizer/BioNTech",
            "moderna": "Moderna",
        }
        assert set(df["vaccine"].unique()) == set(vaccine_mapping.keys())
        return (
            df.rename(columns={"sumTotal": "total_vaccinations"})[df.geoRegion == "CH"]
            .drop(columns="geoRegion")
            .assign(location="Switzerland")
            .replace(vaccine_mapping)
        )

    def to_csv(self, paths):
        vaccine_data, manufacturer_data = self.read()

        vaccine_data.pipe(self.pipeline, country_code="CH").to_csv(
            paths.tmp_vax_out("Switzerland"), index=False
        )

        vaccine_data.pipe(self.pipeline, country_code="FL").to_csv(
            paths.tmp_vax_out("Liechtenstein"), index=False
        )

        df_man = manufacturer_data.pipe(self.pipeline_manufacturer)
        df_man.to_csv(paths.tmp_vax_out_man("Switzerland"), index=False)
        export_metadata(
            df_man,
            "Federal Office of Public Health",
            self.source_url,
            paths.tmp_vax_metadata_man,
        )


def main(paths):
    Switzerland().to_csv(paths)
