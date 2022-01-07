from datetime import datetime

import pandas as pd

import cowidev.megafile.generate
from cowidev.grapher.db.base import GrapherBaseUpdater
from cowidev.utils.utils import time_str_grapher, get_filename, export_timestamp
from cowidev.utils.clean.dates import DATE_FORMAT

ZERO_DAY = "2020-01-21"
zero_day = datetime.strptime(ZERO_DAY, DATE_FORMAT)

URL_VACCINE = "https://raw.githubusercontent.com/OxCGRT/covid-policy-tracker/master/data/OxCGRT_vaccines_full.csv"


def _owid_format(df):
    print("Reshaping to OWID format…")
    df.loc[:, "value"] = df["value"].round(3)
    df = df.drop(columns="iso_code")

    # Data cleaning
    df = df[-df["indicator"].str.contains("Weekly new plot admissions")]
    df["date"] = df.date.astype(str).str.slice(0, 10)
    df = df.groupby(["entity", "date", "indicator"], as_index=False).max()

    df = df.pivot_table(index=["entity", "date"], columns="indicator").value.reset_index()
    df = df.rename(columns={"entity": "Country"})
    return df


def _date_to_owid_year(df):
    print("Converting dates to grapher years…")
    df.loc[:, "date"] = (pd.to_datetime(df.date, format="%Y-%m-%d") - zero_day).dt.days
    df = df.rename(columns={"date": "Year"})
    return df


def run_grapheriser(input_path: str, output_path: str):
    df = pd.read_csv(input_path)
    df = df.pipe(_owid_format).pipe(_date_to_owid_year)
    df = df.drop_duplicates(keep=False, subset=["Country", "Year"])
    df.to_csv(output_path, index=False)
    export_timestamp("owid-covid-data-last-updated-timestamp-hosp.txt")

    print("Generating megafile…")
    cowidev.megafile.generate.generate_megafile()


def run_db_updater(input_path: str):
    dataset_name = get_filename(input_path)
    GrapherBaseUpdater(
        dataset_name=dataset_name,
        source_name=(
            "Official data collated by Our World in Data – Last updated" f" {time_str_grapher()} (London time)"
        ),
        zero_day=ZERO_DAY,
        slack_notifications=True,
    ).run()
