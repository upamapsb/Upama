from glob import glob
import os

import pandas as pd

from cowidev.utils import paths
from cowidev.utils.clean.dates import clean_date_series
from cowidev.utils.utils import check_known_columns
from cowidev.vax.utils.files import export_metadata_manufacturer
from cowidev.vax.utils.utils import build_vaccine_timeline


class UnitedStates:
    def __init__(self):
        self.source_url = "https://data.cdc.gov/api/views/rh2h-3yt2/rows.csv?accessType=DOWNLOAD"
        self.source_url_ref = (
            "https://data.cdc.gov/Vaccinations/COVID-19-Vaccination-Trends-in-the-United-States-N/rh2h-3yt2"
        )
        self.source_url_age = "https://data.cdc.gov/resource/km4m-vcsb.json"
        self.location = "United States"

    ### Main processing ###

    def read(self) -> pd.DataFrame:
        df = pd.read_csv(self.source_url)
        check_known_columns(
            df,
            [
                "Date",
                "MMWR_week",
                "Location",
                "Administered_Daily",
                "Administered_Cumulative",
                "Administered_7_Day_Rolling_Average",
                "Admin_Dose_1_Daily",
                "Admin_Dose_1_Cumulative",
                "Admin_Dose_1_Day_Rolling_Average",
                "date_type",
                "Administered_daily_change_report",
                "Administered_daily_change_report_7dayroll",
                "Series_Complete_Daily",
                "Series_Complete_Cumulative",
                "Series_Complete_Day_Rolling_Average",
                "Booster_Daily",
                "Booster_Cumulative",
                "Booster_7_Day_Rolling_Average",
            ],
        )
        return df[
            [
                "Date",
                "Location",
                "Administered_Cumulative",
                "Admin_Dose_1_Cumulative",
                "date_type",
                "Series_Complete_Cumulative",
                "Booster_Cumulative",
            ]
        ]

    def pipe_filter_rows(self, df: pd.DataFrame) -> pd.DataFrame:
        return df[(df.Location == "US") & (df.date_type == "Admin")]

    def pipe_clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        df = (
            df.assign(Date=clean_date_series(df.Date, format_input="%m/%d/%Y"), Location="United States")
            .drop(columns=["date_type"])
            .rename(
                columns={
                    "Date": "date",
                    "Location": "location",
                    "Administered_Cumulative": "total_vaccinations",
                    "Admin_Dose_1_Cumulative": "people_vaccinated",
                    "Series_Complete_Cumulative": "people_fully_vaccinated",
                    "Booster_Cumulative": "total_boosters",
                }
            )
            .sort_values("date")
        )
        return df[df.total_vaccinations > 0].drop_duplicates(subset=["date"], keep=False)

    def pipe_add_source(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(source_url=self.source_url_ref)

    def pipe_add_vaccines(self, df: pd.DataFrame) -> pd.DataFrame:
        schedule = {
            "Pfizer/BioNTech": "2020-12-01",
            "Moderna": "2020-12-23",
            "Johnson&Johnson": "2021-03-05",
        }
        return build_vaccine_timeline(df, schedule)

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.pipe(self.pipe_filter_rows)
            .pipe(self.pipe_clean_data)
            .pipe(self.pipe_add_source)
            .pipe(self.pipe_add_vaccines)
        )

    ### Manufacturer processing ###

    def read_manufacturer(self) -> pd.DataFrame:
        vaccine_cols = [
            "Administered_Pfizer",
            "Administered_Moderna",
            "Administered_Janssen",
        ]
        dfs = []
        for file in glob(os.path.join(paths.SCRIPTS.INPUT_CDC_VAX, "cdc_data_*.csv")):
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

    def export(self):
        self.read().pipe(self.pipeline).to_csv(paths.out_vax(self.location), index=False)

        df_manufacturer = self.read_manufacturer()
        df_manufacturer.to_csv(paths.out_vax(self.location, manufacturer=True), index=False)
        export_metadata_manufacturer(
            df_manufacturer,
            "Centers for Disease Control and Prevention",
            "https://covid.cdc.gov/covid-data-tracker/COVIDData/getAjaxData?id=vaccination_data",
        )


def main():
    UnitedStates().export()


if __name__ == "__main__":
    main()
