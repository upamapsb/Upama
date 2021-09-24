import json

import requests
import pandas as pd

from cowidev.vax.utils.files import export_metadata


class Lithuania:
    location: str = "Lithuania"
    source_url: str = (
        "https://services3.arcgis.com/MF53hRPmwfLccHCj/arcgis/rest/services/"
        "covid_vaccinations_by_drug_name_new/FeatureServer/0/query"
    )
    source_url_ref: str = "https://experience.arcgis.com/experience/cab84dcfe0464c2a8050a78f817924ca/page/page_3/"
    query_params: dict = {
        "f": "json",
        "where": "municipality_code='00'",
        "returnGeometry": False,
        "spatialRel": "esriSpatialRelIntersects",
        "outFields": "date,vaccine_name,vaccination_state,vaccinated_cum",
        "resultOffset": 0,
        "resultRecordCount": 32000,
        "resultType": "standard",
    }

    def read(self):
        res = requests.get(self.source_url, params=self.query_params)
        if res.ok:
            data = [elem["attributes"] for elem in json.loads(res.content)["features"]]
            return pd.DataFrame.from_records(data)
        raise ValueError("Source not valid/available!")

    def pipe_date(self, df: pd.DataFrame) -> pd.DataFrame:
        df["date"] = pd.to_datetime(df["date"], unit="ms")
        # Correction for vaccinations wrongly attributed to early December 2020
        df.loc[df.date < "2020-12-27", "date"] = pd.to_datetime("2020-12-27")
        return df

    def pipe_reshape(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df[(df.vaccination_state != "Dalinai") & (df.vaccinated_cum > 0)].copy()
        df.loc[df.vaccination_state == "Visi", "dose_number"] = 1
        df.loc[df.vaccination_state == "Pilnai", "dose_number"] = 2
        return df.drop(columns="vaccination_state")

    def pipe_vaccine(self, df: pd.DataFrame) -> pd.DataFrame:
        vaccine_mapping = {
            "Pfizer-BioNTech": "Pfizer/BioNTech",
            "Moderna": "Moderna",
            "AstraZeneca": "Oxford/AstraZeneca",
            "Johnson & Johnson": "Johnson&Johnson",
        }
        vaccines_wrong = set(df["vaccine_name"].unique()).difference(vaccine_mapping)
        if vaccines_wrong:
            raise ValueError(f"Missing vaccines: {vaccines_wrong}")
        df = df.replace(vaccine_mapping)
        return df

    def pipeline_base(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.pipe(self.pipe_date).pipe(self.pipe_reshape).pipe(self.pipe_vaccine)

    def pipeline_manufacturer(self, df: pd.DataFrame) -> pd.DataFrame:
        df = (
            df.groupby(["date", "vaccine_name"], as_index=False)["vaccinated_cum"]
            .sum()
            .sort_values("date")
            .rename(columns={"vaccine_name": "vaccine", "vaccinated_cum": "total_vaccinations"})
        )
        df["location"] = self.location
        return df

    def pipe_unpivot(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.groupby(["date", "dose_number", "vaccine_name"], as_index=False)
            .sum()
            .pivot(
                index=["date", "vaccine_name"],
                columns="dose_number",
                values="vaccinated_cum",
            )
            .fillna(0)
            .reset_index()
            .rename(columns={1: "people_vaccinated", 2: "people_fully_vaccinated"})
            .sort_values("date")
        )

    def pipe_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.assign(total_vaccinations=df.people_vaccinated + df.people_fully_vaccinated)
        # Single shot
        msk = df.vaccine_name == "Johnson & Johnson"
        df.loc[msk, "people_fully_vaccinated"] = df.loc[msk, "people_vaccinated"]
        return df

    def pipe_aggregate(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.groupby("date")
            .agg(
                {
                    "people_fully_vaccinated": sum,
                    "people_vaccinated": sum,
                    "total_vaccinations": sum,
                    "vaccine_name": lambda x: ", ".join(sorted(x)),
                }
            )
            .rename(columns={"vaccine_name": "vaccine"})
            .reset_index()
            .replace(0, pd.NA)
        )

    def pipe_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(
            location=self.location,
            source_url=self.source_url_ref,
        )

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.pipe(self.pipe_unpivot).pipe(self.pipe_metrics).pipe(self.pipe_aggregate).pipe(self.pipe_metadata)

    def export(self, paths):
        df_base = self.read().pipe(self.pipeline_base)
        # Manufacturer
        df_man = df_base.pipe(self.pipeline_manufacturer)
        df_man.to_csv(paths.tmp_vax_out_man("Lithuania"), index=False)
        export_metadata(df_man, "Ministry of Health", self.source_url_ref, paths.tmp_vax_metadata_man)
        # Main
        df = df_base.pipe(self.pipeline)
        df.to_csv(paths.tmp_vax_out(self.location), index=False)


def main(paths):
    Lithuania().export(paths)
