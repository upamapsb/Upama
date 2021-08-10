from datetime import datetime

import requests
import pandas as pd

from cowidev.vax.utils.dates import clean_date
from cowidev.vax.utils.incremental import increment
from cowidev.vax.utils.who import VACCINES_WHO_MAPPING
from cowidev.vax.cmd.utils import get_logger


logger = get_logger()

# Dict mapping Africa CDC country names -> OWID country names
COUNTRIES = {
    "Central African Republic": "Central African Republic",
    "Chad": "Chad",
    "Congo": "Congo",
    "Djibouti": "Djibouti",
    "Gabon": "Gabon",
    "Gambia": "Gambia",
    "Mauritania": "Mauritania",
}


class AfricaCDC:
    def __init__(self) -> None:
        self._base_url = (
            "https://services8.arcgis.com/vWozsma9VzGndzx7/ArcGIS/rest/services/"
            "Admin_Boundaries_Africa_corr_Go_Vaccine_DB_JOIN/FeatureServer/0"
        )
        self.source_url_ref = "https://africacdc.org/covid-19-vaccination/"
        self.vaccines_mapping = {
            "AstraZeneca": "Oxford/AstraZeneca",
            "Sinopharm": "Sinopharm/Beijing",
            "Sputnik V": "Sputnik V",
            "Sinovac": "Sinovac",
            "BioNTech": "Pfizer/BioNTech",
            "Moderna": "Moderna",
            "J&J": "Johnson&Johnson",
            "Covaxin": "Covaxin",
        }

    @property
    def source_url(self):
        return f"{self._base_url}/query?f=json&where=1=1&outFields=*"

    @property
    def source_url_date(self):
        return f"{self._base_url}?f=pjson"

    def read(self) -> pd.DataFrame:
        data = requests.get(self.source_url).json()
        res = [d["attributes"] for d in data["features"]]
        df = pd.DataFrame(
            res,
            columns=[
                "ADM0_SOVRN",
                "ISO_3_CODE",
                "TotAmtAdmi",
                "VacAd1Dose",
                "VacAd2Dose",
                "FullyVacc",
                "VaccApprov",
            ],
        )
        return df

    def pipe_rename(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.rename(
            columns={
                "ADM0_SOVRN": "location",
                "TotAmtAdmi": "total_vaccinations",
                "FullyVacc": "people_fully_vaccinated",
                "VacAd1Dose": "people_vaccinated",
            }
        )

    def pipe_filter_countries(self, df: pd.DataFrame) -> pd.DataFrame:
        """Get rows from selected countries."""
        df["location"] = df.location.replace(COUNTRIES)
        df = df[df.location.isin(COUNTRIES)]
        return df

    def pipe_one_dose_correction(self, df: pd.DataFrame) -> pd.DataFrame:
        single_shot = df.people_fully_vaccinated - df.VacAd2Dose
        return df.assign(people_vaccinated=df.people_vaccinated + single_shot)

    def pipe_vaccine(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(vaccine=df.VaccApprov.apply(self._map_vaccines))

    def _map_vaccines(self, vaccine_raw: str):
        vaccine_raw = vaccine_raw.strip()
        vaccines = []
        for vax_old, vax_new in self.vaccines_mapping.items():
            if vax_old in vaccine_raw:
                vaccines.append(vax_new)
                vaccine_raw = vaccine_raw.replace(vax_old, "").strip()
            if vaccine_raw == "":
                break
        if vaccine_raw != "":
            raise ValueError(f"Some vaccines were unknown {vaccine_raw}")
        vaccines = ", ".join(sorted(vaccines))
        return vaccines

    def pipe_vaccine_who(self, df: pd.DataFrame) -> pd.DataFrame:
        url = "https://covid19.who.int/who-data/vaccination-data.csv"
        df_who = pd.read_csv(url, usecols=["ISO3", "VACCINES_USED"]).rename(
            columns={"VACCINES_USED": "vaccine"}
        )
        df_who = df_who.dropna(subset=["vaccine"])
        df_who = df_who.assign(
            vaccine=df_who.vaccine.apply(
                lambda x: ", ".join(
                    sorted(set(VACCINES_WHO_MAPPING[xx.strip()] for xx in x.split(",")))
                )
            )
        )
        df = df.merge(df_who, left_on="ISO_3_CODE", right_on="ISO3")
        return df

    def pipe_source(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(source_url=self.source_url_ref)

    def pipe_date(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(date=self._parse_date())

    def _parse_date(self):
        res = requests.get(self.source_url_date).json()
        edit_ts = res["editingInfo"]["lastEditDate"]
        return clean_date(datetime.fromtimestamp(edit_ts / 1000))

    def pipe_select_out_cols(self, df: pd.DataFrame) -> pd.DataFrame:
        return df[
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

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.pipe(self.pipe_rename)
            .pipe(self.pipe_filter_countries)
            .pipe(self.pipe_one_dose_correction)
            .pipe(self.pipe_vaccine_who)
            .pipe(self.pipe_source)
            .pipe(self.pipe_date)
            .pipe(self.pipe_select_out_cols)
        )

    def increment_countries(self, df: pd.DataFrame, paths):
        for row in df.sort_values("location").iterrows():
            row = row[1]
            increment(
                paths=paths,
                location=row["location"],
                total_vaccinations=row["total_vaccinations"],
                people_vaccinated=row["people_vaccinated"],
                people_fully_vaccinated=row["people_fully_vaccinated"],
                date=row["date"],
                vaccine=row["vaccine"],
                source_url=row["source_url"],
            )
            country = row["location"]
            logger.info(f"\tvax.incremental.africacdc.{country}: SUCCESS ✅")

    def export(self, paths):
        df = self.read().pipe(self.pipeline)
        self.increment_countries(df, paths)


def main(paths):
    AfricaCDC().export(paths)
