import datetime

import pandas as pd

from cowidev.utils.clean import clean_count
from cowidev.utils.web.scraping import get_soup
from cowidev.vax.utils.utils import make_monotonic


class Sweden(object):
    def __init__(self):
        """Constructor."""
        self.source_url_daily = (
            "https://fohm.se/smittskydd-beredskap/utbrott/aktuella-utbrott/covid-19/statistik-och-analyser/"
            "statistik-over-registrerade-vaccinationer-covid-19/"
        )
        self.source_url_weekly = (
            "https://fohm.maps.arcgis.com/sharing/rest/content/items/fc749115877443d29c2a49ea9eca77e9/data"
        )
        self.location = "Sweden"
        self.columns_rename = None

    def read(self) -> pd.DataFrame:
        daily = self._read_daily_data()
        weekly = self._read_weekly_data()
        weekly = weekly[weekly["date"] < daily["date"].min()]
        return pd.concat([daily, weekly]).sort_values("date").reset_index(drop=True)

    def pipe_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(location=self.location, source_url=self.source_url_daily)

    def pipe_vaccine(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(vaccine="Moderna, Oxford/AstraZeneca, Pfizer/BioNTech")

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.pipe(self.pipe_vaccine).pipe(self.pipe_columns).pipe(self.pipe_out_columns).pipe(make_monotonic)

    def _week_to_date(self, row: int):
        origin_date = pd.to_datetime("2019-12-29") if row.Vecka >= 52 else pd.to_datetime("2021-01-03")
        return origin_date + pd.DateOffset(days=7 * int(row.Vecka))

    def _read_weekly_data(self) -> pd.DataFrame:
        df = pd.read_excel(self.source_url_weekly, sheet_name="Vaccinerade tidsserie")
        df = df[df["Region"] == "| Sverige |"][["Vecka", "Antal vaccinerade", "Vaccinationsstatus"]]
        df = df.pivot_table(values="Antal vaccinerade", index="Vecka", columns="Vaccinationsstatus").reset_index()
        # Week-to-date logic will stop working after 2021
        if not datetime.date.today().year < 2022:
            raise ValueError("Check the year! This script is not ready for 2022!")
        df.loc[:, "date"] = df.apply(self._week_to_date, axis=1).dt.date.astype(str)
        df = (
            df.drop(columns=["Vecka"])
            .sort_values("date")
            .rename(
                columns={
                    "Minst 1 dos": "people_vaccinated",
                    "Färdigvaccinerade": "people_fully_vaccinated",
                }
            )
        )
        df.loc[:, "total_vaccinations"] = df["people_vaccinated"] + df["people_fully_vaccinated"]
        return df

    def _read_daily_data(self) -> pd.DataFrame:
        dfs = pd.read_html(self.source_url_daily, encoding="utf-8")
        df = self._get_df_daily(dfs[1])
        df_doses = self._get_df_doses_daily(dfs[0])
        df = self._merge_tables_daily(df, df_doses)
        return df

    def _read_daily_data_split(self) -> pd.DataFrame:
        dfs = pd.read_html(self.source_url_daily, encoding="utf-8")
        df_adults = self._get_df_adults_daily(dfs[1])
        df_teens = self._get_df_teens_daily(dfs[2])
        df_doses = self._get_df_doses_daily(dfs[0])
        df = self._merge_tables_daily_split(df_adults, df_teens, df_doses)
        return df

    def _get_df_daily(self, df):
        return df.assign(
            people_vaccinated=df["Antal vaccinerademed minst 1 dos*"].apply(clean_count),
            people_fully_vaccinated=df["Antal vaccinerademed 2 doser"].apply(clean_count),
        )

    def _get_df_adults_daily(self, df):
        # People vaccinated >18 yo
        return df.assign(
            people_vaccinated=df["Antal vaccinerademed minst 1 dos*"].apply(clean_count),
            people_fully_vaccinated=df["Antal vaccinerademed 2 doser"].apply(clean_count),
        )

    def _get_df_teens_daily(self, df):
        # People vaccinated < 18 yo
        df_teens = df.pivot("Datum", "Status", "Antal vaccinerade födda 2003-2005").reset_index()
        return df_teens.assign(
            people_vaccinated=df_teens["Minst 1 dos"].apply(clean_count),
            people_fully_vaccinated=df_teens["2 doser"].apply(clean_count),
        )

    def _get_df_doses_daily(self, df):
        # Total vaccinations
        return df.assign(
            total_vaccinations=df["Antal vaccinationer"].apply(clean_count),
        )

    def _merge_tables_daily(self, df, df_doses):
        # Merge
        df = df.merge(df_doses, on="Datum").rename(
            columns={
                "Datum": "date",
            }
        )
        df[["people_vaccinated", "people_fully_vaccinated"]] = df[
            ["people_vaccinated", "people_fully_vaccinated"]
        ].astype("Int64")
        df = df[["date", "total_vaccinations", "people_vaccinated", "people_fully_vaccinated"]]
        return df

    def _merge_tables_daily_split(self, df_adults, df_teens, df_doses):
        # Merge people metrics
        df = df_adults.merge(df_teens, on="Datum", how="left")
        df = df.assign(
            people_vaccinated=df.filter(regex="people_vaccinated_*").sum(axis=1),
            people_fully_vaccinated=df.filter(regex="people_fully_vaccinated_*").sum(axis=1),
        )
        # Merge
        df = df.merge(df_doses, on="Datum").rename(
            columns={
                "Datum": "date",
            }
        )
        df[["people_vaccinated", "people_fully_vaccinated"]] = df[
            ["people_vaccinated", "people_fully_vaccinated"]
        ].astype("Int64")
        df = df[["date", "total_vaccinations", "people_vaccinated", "people_fully_vaccinated"]]
        return df

    def pipe_out_columns(self, df: pd.DataFrame):
        return df[
            [
                "date",
                "people_vaccinated",
                "people_fully_vaccinated",
                "total_vaccinations",
                "vaccine",
                "location",
                "source_url",
            ]
        ]

    def export(self, paths):
        """Generalized."""
        df = self.read().pipe(self.pipeline)
        df.to_csv(paths.tmp_vax_out(self.location), index=False)


def main(paths):
    Sweden().export(paths)
