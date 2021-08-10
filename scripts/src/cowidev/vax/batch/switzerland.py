import os
import tempfile
from zipfile import ZipFile

import pandas as pd

from cowidev.vax.utils.utils import get_driver, download_file_from_url
from cowidev.vax.utils.files import export_metadata


class Switzerland:
    def __init__(self):
        self.location = "Switzerland"
        self.source_url = "https://www.covid19.admin.ch/en/epidemiologic/vacc-doses"

    def read(self):
        zip_url = self._parse_file_url()
        df, df_manufacturer = self._parse_data(zip_url)
        return df, df_manufacturer

    def _parse_file_url(self) -> str:
        with get_driver() as driver:
            driver.get(self.source_url)
            elems = driver.find_elements_by_class_name("footer__nav__link")
            for elem in elems:
                if "Data as .csv file" == elem.text:
                    return elem.get_attribute("href")
        raise Exception("No CSV link found in footer.")

    def _parse_data(self, url):
        with tempfile.TemporaryDirectory(dir=".") as temp_dir:
            zip_path = os.path.join(temp_dir, "file.zip")
            download_file_from_url(url, zip_path)
            with ZipFile(zip_path, "r") as zipObj:
                # Extract all the contents of zip file in current directory
                zipObj.extractall(temp_dir)
            doses = pd.read_csv(
                os.path.join(temp_dir, "data/COVID19VaccDosesAdministered.csv"),
                usecols=["geoRegion", "date", "sumTotal", "type"],
            )
            people = pd.read_csv(
                os.path.join(temp_dir, "data/COVID19VaccPersons_v2.csv"),
                usecols=["geoRegion", "date", "sumTotal", "type"],
            )
            manufacturer = pd.read_csv(
                os.path.join(temp_dir, "data/COVID19AdministeredDoses_vaccine.csv"),
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
