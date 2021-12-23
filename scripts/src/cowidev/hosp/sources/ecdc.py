import os
import pandas as pd

from cowidev.utils.utils import get_project_dir


POPULATION = pd.read_csv(
    os.path.join(get_project_dir(), "scripts", "input", "un", "population_latest.csv"),
    usecols=["iso_code", "entity", "population"],
)
SOURCE_URL = "https://opendata.ecdc.europa.eu/covid19/hospitalicuadmissionrates/csv/data.csv"
EXCLUDED_COUNTRIES = [
    "Austria",
    "France",
    "Germany",
]


def download_data():
    print("Downloading ECDC data…")
    df = pd.read_csv(SOURCE_URL, usecols=["country", "indicator", "date", "value", "year_week"])
    df = df[-df.country.isin(EXCLUDED_COUNTRIES)]
    df = df.drop_duplicates()
    df = df.rename(columns={"country": "entity"})
    return df


def pipe_undo_100k(df):
    df = pd.merge(df, POPULATION, on="entity", how="left")
    assert df[df.population.isna()].shape[0] == 0, "Country missing from population file"
    df.loc[df["indicator"].str.contains(" per 100k"), "value"] = df["value"].div(100000).mul(df["population"])
    df.loc[:, "indicator"] = df["indicator"].str.replace(" per 100k", "")
    return df


def pipe_week_to_date(df):
    if df.date.dtypes == "int64":
        df["date"] = pd.to_datetime(df.date, format="%Y%m%d").dt.date
    daily_records = df[df["indicator"].str.contains("Daily")]
    date_week_mapping = daily_records[["year_week", "date"]].groupby("year_week", as_index=False).max()
    weekly_records = df[df["indicator"].str.contains("Weekly")].drop(columns="date")
    weekly_records = pd.merge(weekly_records, date_week_mapping, on="year_week")
    df = pd.concat([daily_records, weekly_records]).drop(columns="year_week")
    return df


def main():
    df = download_data()
    df = df.pipe(pipe_undo_100k).pipe(pipe_week_to_date)
    return df
