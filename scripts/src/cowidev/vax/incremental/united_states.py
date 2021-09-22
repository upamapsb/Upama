import os
import pandas as pd
from glob import glob
import re

from cowidev.utils.clean import clean_date, clean_date_series
from cowidev.utils.web import request_json
from cowidev.vax.utils.incremental import enrich_data, increment
from cowidev.vax.utils.files import export_metadata


vaccines_mapping = {
    "Pfizer": "Pfizer/BioNTech",
    "Janssen": "Johnson&Johnson",
    "Moderna": "Moderna",
}


class UnitedStates:
    def __init__(self):
        self.source_url = "https://covid.cdc.gov/covid-data-tracker/COVIDData/getAjaxData?id=vaccination_data"
        self.source_url_age = "https://data.cdc.gov/resource/km4m-vcsb.json"
        self.location = "United States"

    def read(self) -> pd.Series:
        data = self._parse_data()
        # Build Series
        return pd.Series(
            {
                "total_vaccinations": data["Doses_Administered"],
                "people_vaccinated": data["Administered_Dose1_Recip"],
                "people_fully_vaccinated": data["Series_Complete_Yes"],
                "total_boosters": data["additional_doses"],
                "date": clean_date(data["Date"], "%Y-%m-%d"),
                "vaccine": self._parse_vaccines(data),
            }
        )

    def _parse_data(self):
        # Request data
        data = request_json(self.source_url)
        data = data["vaccination_data"]
        # Get only US data (total)
        data = [d for d in data if d["ShortName"] == "USA"]
        if len(data) != 1:
            raise ValueError('More than one data element with ShortName == "USA". Check source data!')
        return data[0]

    def _parse_vaccines(self, data: dict):
        r = re.compile(r"Administered_([a-zA-Z]+)")
        vaccines = set([re.fullmatch(r, k).group(1) for k in data.keys() if re.fullmatch(r, k)])
        vaccines_wrong = vaccines.difference(vaccines_mapping)
        if vaccines_wrong:
            raise ValueError(f"Missing vaccines: {vaccines_wrong}")
        return ", ".join(sorted((vaccines_mapping[vax] for vax in vaccines)))

    def pipe_location(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "location", self.location)

    def pipe_source(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "source_url", self.source_url)

    def pipeline(self, ds: pd.Series) -> pd.Series:
        return ds.pipe(self.pipe_location).pipe(self.pipe_source)

    def read_manufacturer(self, paths) -> pd.DataFrame:
        vaccine_cols = [
            "Administered_Pfizer",
            "Administered_Moderna",
            "Administered_Janssen",
        ]
        dfs = []
        for file in glob(os.path.join(paths.in_us_states, "cdc_data_*.csv")):
            try:
                df = pd.read_csv(file)
                for vc in vaccine_cols:
                    if vc not in df.columns:
                        df[vc] = pd.NA
                df = df[["Date", "LongName"] + vaccine_cols]
                dfs.append(df)
            except Exception:
                continue
        df = pd.concat(dfs)
        df = (
            df[df.LongName == "United States"]
            .sort_values("Date")
            .rename(
                columns={
                    "Date": "date",
                    "LongName": "location",
                    "Administered_Pfizer": "Pfizer/BioNTech",
                    "Administered_Moderna": "Moderna",
                    "Administered_Janssen": "Johnson&Johnson",
                }
            )
        )
        df = df.melt(["date", "location"], var_name="vaccine", value_name="total_vaccinations")
        df = df.dropna(subset=["total_vaccinations"])
        return df

    def read_age(self) -> pd.DataFrame:
        data = request_json(self.source_url_age)
        age_groups_accepted = {
            #     'Ages_<12yrs',
            #     'Ages_12-15_yrs',
            #     'Ages_16-17_yrs',
            "Ages_<18yrs": "0-18",
            "Ages_18-29_yrs": "1-29",
            "Ages_30-39_yrs": "30-39",
            "Ages_40-49_yrs": "40-49",
            "Ages_50-64_yrs": "50-64",
            "Ages_65-74_yrs": "65-74",
            "Ages_75+_yrs": "75-",
        }
        res = filter(
            lambda x: x["demographic_category"] in age_groups_accepted,
            [d for d in data],
        )
        df = pd.DataFrame(
            res,
            columns=[
                "date",
                "demographic_category",
                "administered_dose1",
                "series_complete_yes",
            ],
        ).astype({"administered_dose1": int, "series_complete_yes": int})
        df = df.assign(demographic_category=df.demographic_category.replace(age_groups_accepted))
        return df

    def pipe_age_rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.rename(
            columns={
                "administered_dose1": "people_vaccinated",
                "series_complete_yes": "people_fully_vaccinated",
                "demographic_category": "age_group",
            }
        )

    def pipe_age_date(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(date=clean_date_series(df.date, "%Y-%m-%dT%H:%M:%S.000"))

    def pipe_age_location(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(location=self.location)

    def pipe_age_minmax_values(self, df: pd.DataFrame) -> pd.DataFrame:
        df[["age_group_min", "age_group_max"]] = df.age_group.str.split("-", expand=True)
        return df

    def pipe_age_total_vaccinations(self, df: pd.DataFrame) -> pd.DataFrame:
        df.assign(total_vaccinations=df.people_vaccinated + df.people_fully_vaccinated)

    def pipeline_age(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.pipe(self.pipe_age_rename_columns)
            .pipe(self.pipe_age_location)
            .pipe(self.pipe_age_minmax_values)
            .pipe(self.pipe_age_date)
            # .pipe(self.pipe_age_total_vaccinations)
            [
                [
                    "location",
                    "date",
                    "age_group_min",
                    "age_group_max",
                    "people_vaccinated",
                ]
            ]
            .sort_values(["location", "date", "age_group_min"])
        )

    def to_csv(self, paths):
        """Generalized."""
        # Main data
        data = self.read().pipe(self.pipeline)
        increment(
            paths=paths,
            location=data["location"],
            total_vaccinations=data["total_vaccinations"],
            people_vaccinated=data["people_vaccinated"],
            people_fully_vaccinated=data["people_fully_vaccinated"],
            total_boosters=data["total_boosters"],
            date=data["date"],
            source_url=data["source_url"],
            vaccine=data["vaccine"],
        )
        # Vaccination by manufacturer
        df_manufacturer = self.read_manufacturer(paths)
        df_manufacturer.to_csv(paths.tmp_vax_out_man(self.location), index=False)
        export_metadata(
            df_manufacturer,
            "Centers for Disease Control and Prevention",
            self.source_url,
            paths.tmp_vax_metadata_man,
        )
        # Vaccination by age group
        # df_age = self.read_age().pipe(self.pipeline_age)
        # df_age.to_csv(paths.tmp_vax_out_by_age_group(self.location), index=False)


def main(paths):
    UnitedStates().to_csv(paths)


if __name__ == "__main__":
    main()
