from datetime import datetime, date
import json
import os
import requests
import sys

import numpy as np
import pandas as pd
import pytz


CURRENT_DIR = os.path.dirname(__file__)
sys.path.append(CURRENT_DIR)

from utils.db_imports import import_dataset
from utils.utils import export_timestamp


SOURCE_URL = "https://opendata.ecdc.europa.eu/covid19/hospitalicuadmissionrates/csv/data.csv"
INPUT_PATH = os.path.join(CURRENT_DIR, "../input/")
TIMESTAMP_PATH = os.path.join(CURRENT_DIR, "../../public/data/internal/timestamp/")
GRAPHER_PATH = os.path.join(CURRENT_DIR, "../grapher/")
DATASET_NAME = "COVID-2019 - Hospital & ICU"
ZERO_DAY = "2020-01-21"
POPULATION = pd.read_csv(
    os.path.join(INPUT_PATH, "un/population_2020.csv"),
    usecols=["iso_code", "entity", "population"],
)


def download_data():
    print("Downloading ECDC data…")
    df = pd.read_csv(SOURCE_URL, usecols=["country", "indicator", "date", "value", "year_week"])
    df = df.drop_duplicates()
    df = df.rename(columns={"country": "entity"})
    return df


def standardize_entities(df):
    return df


def undo_per_100k(df):
    df = pd.merge(df, POPULATION, on="entity", how="left")
    assert df[df.population.isna()].shape[0] == 0, "Country missing from population file"
    df.loc[df["indicator"].str.contains(" per 100k"), "value"] = df["value"].div(100000).mul(df["population"])
    df.loc[:, "indicator"] = df["indicator"].str.replace(" per 100k", "")
    return df


def week_to_date(df):
    if df.date.dtypes == "int64":
        df["date"] = pd.to_datetime(df.date, format="%Y%m%d").dt.date
    daily_records = df[df["indicator"].str.contains("Daily")]
    date_week_mapping = daily_records[["year_week", "date"]].groupby("year_week", as_index=False).max()
    weekly_records = df[df["indicator"].str.contains("Weekly")].drop(columns="date")
    weekly_records = pd.merge(weekly_records, date_week_mapping, on="year_week")
    df = pd.concat([daily_records, weekly_records]).drop(columns="year_week")
    return df


def add_united_states(df):
    print("Downloading US data…")
    url = "https://healthdata.gov/api/views/g62h-syeh/rows.csv"

    usa = pd.read_csv(
        url,
        usecols=[
            "date",
            "total_adult_patients_hospitalized_confirmed_covid",
            "total_pediatric_patients_hospitalized_confirmed_covid",
            "staffed_icu_adult_patients_confirmed_covid",
            "previous_day_admission_adult_covid_confirmed",
            "previous_day_admission_pediatric_covid_confirmed",
        ],
    )

    usa.loc[:, "date"] = pd.to_datetime(usa.date)
    usa = usa[usa.date >= pd.to_datetime("2020-07-15")]
    usa = usa.groupby("date", as_index=False).sum()

    stock = usa[
        [
            "date",
            "total_adult_patients_hospitalized_confirmed_covid",
            "total_pediatric_patients_hospitalized_confirmed_covid",
            "staffed_icu_adult_patients_confirmed_covid",
        ]
    ].copy()
    stock.loc[:, "Daily hospital occupancy"] = stock.total_adult_patients_hospitalized_confirmed_covid.add(
        stock.total_pediatric_patients_hospitalized_confirmed_covid
    )
    stock = stock.rename(columns={"staffed_icu_adult_patients_confirmed_covid": "Daily ICU occupancy"})
    stock = stock[["date", "Daily hospital occupancy", "Daily ICU occupancy"]]
    stock = stock.melt(id_vars="date", var_name="indicator")
    stock.loc[:, "date"] = stock["date"].dt.date

    flow = usa[
        [
            "date",
            "previous_day_admission_adult_covid_confirmed",
            "previous_day_admission_pediatric_covid_confirmed",
        ]
    ].copy()
    flow.loc[:, "value"] = flow.previous_day_admission_adult_covid_confirmed.add(
        flow.previous_day_admission_pediatric_covid_confirmed
    )
    flow.loc[:, "date"] = (flow["date"] + pd.to_timedelta(6 - flow["date"].dt.dayofweek, unit="d")).dt.date
    flow = flow[flow["date"] <= date.today()]
    flow = flow[["date", "value"]]
    flow = flow.groupby("date", as_index=False).agg({"value": ["sum", "count"]})
    flow.columns = ["date", "value", "count"]
    flow = flow[flow["count"] == 7]
    flow = flow.drop(columns="count")
    flow.loc[:, "indicator"] = "Weekly new hospital admissions"

    # Merge all subframes
    usa = pd.concat([stock, flow])

    usa.loc[:, "entity"] = "United States"
    usa.loc[:, "iso_code"] = "USA"
    usa.loc[:, "population"] = 332915074

    df = pd.concat([df, usa])
    return df


def add_canada(df):
    print("Downloading Canada data…")
    url = "https://api.covid19tracker.ca/reports?after=2020-03-09"
    data = requests.get(url).json()
    data = json.dumps(data["data"])
    canada = pd.read_json(data, orient="records")
    canada = canada[["date", "total_hospitalizations", "total_criticals"]]
    canada = canada.melt("date", ["total_hospitalizations", "total_criticals"], "indicator")
    canada.loc[:, "indicator"] = canada["indicator"].replace(
        {
            "total_hospitalizations": "Daily hospital occupancy",
            "total_criticals": "Daily ICU occupancy",
        }
    )

    canada.loc[:, "date"] = canada["date"].dt.date
    canada.loc[:, "entity"] = "Canada"
    canada.loc[:, "iso_code"] = "CAN"
    canada.loc[:, "population"] = 38067913

    df = pd.concat([df, canada])
    return df


def add_uk(df):
    print("Downloading UK data…")
    url = "https://api.coronavirus.data.gov.uk/v2/data?areaType=overview&metric=hospitalCases&metric=newAdmissions&metric=covidOccupiedMVBeds&format=csv"
    uk = pd.read_csv(url, usecols=["date", "hospitalCases", "newAdmissions", "covidOccupiedMVBeds"])
    uk.loc[:, "date"] = pd.to_datetime(uk["date"])

    stock = uk[["date", "hospitalCases", "covidOccupiedMVBeds"]].copy()
    stock = stock.melt("date", var_name="indicator")
    stock.loc[:, "date"] = stock["date"].dt.date

    flow = uk[["date", "newAdmissions"]].copy()
    flow.loc[:, "date"] = (flow["date"] + pd.to_timedelta(6 - flow["date"].dt.dayofweek, unit="d")).dt.date
    flow = flow[flow["date"] <= date.today()]
    flow = flow.groupby("date", as_index=False).agg({"newAdmissions": ["sum", "count"]})
    flow.columns = ["date", "newAdmissions", "count"]
    flow = flow[flow["count"] == 7]
    flow = flow.drop(columns="count").melt("date", var_name="indicator")

    uk = pd.concat([stock, flow]).dropna(subset=["value"])
    uk.loc[:, "indicator"] = uk["indicator"].replace(
        {
            "hospitalCases": "Daily hospital occupancy",
            "covidOccupiedMVBeds": "Daily ICU occupancy",
            "newAdmissions": "Weekly new hospital admissions",
        }
    )

    uk.loc[:, "entity"] = "United Kingdom"
    uk.loc[:, "iso_code"] = "GBR"
    uk.loc[:, "population"] = 68207114

    df = pd.concat([df, uk])
    return df


def add_israel(df):
    print("Downloading Israel data…")
    url = "https://datadashboardapi.health.gov.il/api/queries/patientsPerDate"
    israel = pd.read_json(url)
    israel.loc[:, "date"] = pd.to_datetime(israel["date"])

    stock = israel[["date", "Counthospitalized", "CountCriticalStatus"]].copy()
    stock.loc[:, "date"] = stock["date"].dt.date
    stock.loc[stock["date"].astype(str) < "2020-08-17", "CountCriticalStatus"] = np.nan
    stock = stock.melt("date", var_name="indicator")

    flow = israel[["date", "new_hospitalized", "serious_critical_new"]].copy()
    flow.loc[:, "date"] = (flow["date"] + pd.to_timedelta(6 - flow["date"].dt.dayofweek, unit="d")).dt.date
    flow = flow[flow["date"] <= date.today()]
    flow = flow.groupby("date", as_index=False).agg(
        {"new_hospitalized": ["sum", "count"], "serious_critical_new": "sum"}
    )
    flow.columns = ["date", "new_hospitalized", "count", "serious_critical_new"]
    flow = flow[flow["count"] == 7]
    flow = flow.drop(columns="count").melt("date", var_name="indicator")

    israel = pd.concat([stock, flow]).dropna(subset=["value"])
    israel.loc[:, "indicator"] = israel["indicator"].replace(
        {
            "Counthospitalized": "Daily hospital occupancy",
            "CountCriticalStatus": "Daily ICU occupancy",
            "new_hospitalized": "Weekly new hospital admissions",
            "serious_critical_new": "Weekly new ICU admissions",
        }
    )

    israel.loc[:, "entity"] = "Israel"
    israel.loc[:, "iso_code"] = "ISR"
    israel.loc[:, "population"] = 8789776

    return pd.concat([df, israel])


def add_algeria(df):

    print("Downloading Algeria data…")
    url = "https://raw.githubusercontent.com/yasserkaddour/covid19-icu-data-algeria/main/algeria-covid19-icu-data.csv"
    algeria = pd.read_csv(url, usecols=["date", "in_icu"])

    algeria = algeria.melt("date", ["in_icu"], "indicator")
    algeria.loc[:, "indicator"] = algeria["indicator"].replace({"in_icu": "Daily ICU occupancy"})

    algeria.loc[:, "entity"] = "Algeria"
    algeria.loc[:, "iso_code"] = "DZA"
    algeria.loc[:, "population"] = 44616626

    return pd.concat([df, algeria])


def add_switzerland(df: pd.DataFrame) -> pd.DataFrame:

    print("Downloading Switzerland data…")
    context = requests.get("https://www.covid19.admin.ch/api/data/context").json()

    # Hospital & ICU patients
    url = context["sources"]["individual"]["csv"]["daily"]["hospCapacity"]
    stock = pd.read_csv(
        url,
        usecols=[
            "date",
            "geoRegion",
            "type_variant",
            "ICU_Covid19Patients",
            "Total_Covid19Patients",
        ],
    )
    stock = stock[(stock.geoRegion == "CH") & (stock.type_variant == "fp7d")].drop(
        columns=["geoRegion", "type_variant"]
    )
    stock.loc[:, "date"] = pd.to_datetime(stock["date"])

    # Hospital admissions
    url = context["sources"]["individual"]["csv"]["daily"]["hosp"]
    flow = pd.read_csv(url, usecols=["datum", "geoRegion", "entries"])
    flow = flow[flow.geoRegion == "CH"].drop(columns=["geoRegion"]).rename(columns={"datum": "date"})
    flow.loc[:, "date"] = pd.to_datetime(flow["date"])
    flow.loc[:, "date"] = (flow["date"] + pd.to_timedelta(6 - flow["date"].dt.dayofweek, unit="d")).dt.date
    flow = flow[flow["date"] <= date.today()]
    flow = flow.groupby("date", as_index=False).agg({"entries": ["sum", "count"]})
    flow.columns = ["date", "entries", "count"]
    flow = flow[flow["count"] == 7]
    flow = flow.drop(columns="count")
    flow.loc[:, "date"] = pd.to_datetime(flow["date"])

    # Merge
    swiss = pd.merge(stock, flow, on="date", how="outer")
    swiss = swiss.melt("date", ["ICU_Covid19Patients", "Total_Covid19Patients", "entries"], "indicator")
    swiss.loc[:, "indicator"] = swiss["indicator"].replace(
        {
            "ICU_Covid19Patients": "Daily ICU occupancy",
            "Total_Covid19Patients": "Daily hospital occupancy",
            "entries": "Weekly new hospital admissions",
        },
    )

    swiss.loc[:, "entity"] = "Switzerland"
    swiss.loc[:, "iso_code"] = "CHE"
    swiss.loc[:, "population"] = 8715494

    return pd.concat([df, swiss])


def add_singapore(df):
    print("Downloading Singapore data…")

    # Get data
    url = "https://covidsitrep.moh.gov.sg/_dash-layout"
    data = requests.get(url).json()["props"]["children"][1]["props"]["children"][3]["props"]["children"][0]["props"][
        "figure"
    ]["data"]
    data_icu = data[6]

    singapore = pd.DataFrame(
        {
            "date": data_icu["x"],
            "value": data_icu["y"],
        }
    )
    singapore = singapore.assign(
        date=singapore.date.apply(lambda x: datetime.strptime(x, "%Y-%m-%dT%H:%M:%S").strftime("%Y-%m-%d")),
        indicator="Daily ICU occupancy",
        entity="Singapore",
        iso_code="SGP",
        population=5896684,
    )
    return pd.concat([df, singapore])


def add_countries(df):
    df = add_singapore(df)
    df = add_algeria(df)
    df = add_canada(df)
    df = add_israel(df)
    df = add_switzerland(df)
    df = add_uk(df)
    df = add_united_states(df)
    return df


def add_per_million(df):
    per_million = df.copy()
    per_million.loc[:, "value"] = per_million["value"].div(per_million["population"]).mul(1000000)
    per_million.loc[:, "indicator"] = per_million["indicator"] + " per million"
    df = pd.concat([df, per_million]).drop(columns="population")
    return df


def owid_format(df):
    df.loc[:, "value"] = df["value"].round(3)
    df = df.drop(columns="iso_code")

    # Data cleaning
    df = df[-df["indicator"].str.contains("Weekly new plot admissions")]
    df = df.groupby(["entity", "date", "indicator"], as_index=False).max()

    df = df.pivot_table(index=["entity", "date"], columns="indicator").value.reset_index()
    df = df.rename(columns={"entity": "Country"})
    return df


def date_to_owid_year(df):
    df.loc[:, "date"] = (pd.to_datetime(df.date, format="%Y-%m-%d") - datetime(2020, 1, 21)).dt.days
    df = df.rename(columns={"date": "Year"})
    return df


def generate_dataset():
    df = download_data()
    df = standardize_entities(df)
    df = undo_per_100k(df)
    df = week_to_date(df)
    df = add_countries(df)
    df = add_per_million(df)
    df = owid_format(df)
    df = date_to_owid_year(df)
    df = df.drop_duplicates(keep=False, subset=["Country", "Year"])
    df.to_csv(os.path.join(GRAPHER_PATH, "COVID-2019 - Hospital & ICU.csv"), index=False)
    # Timestamp
    filename = os.path.join(TIMESTAMP_PATH, "owid-covid-data-last-updated-timestamp-hosp.txt")
    export_timestamp(filename)


def update_db():
    time_str = datetime.now().astimezone(pytz.timezone("Europe/London")).strftime("%-d %B, %H:%M")
    source_name = (
        f"European CDC for EU countries, government sources for other countries – Last updated {time_str} (London"
        " time)"
    )
    import_dataset(
        dataset_name=DATASET_NAME,
        namespace="owid",
        csv_path=os.path.join(GRAPHER_PATH, DATASET_NAME + ".csv"),
        default_variable_display={"yearIsDay": True, "zeroDay": ZERO_DAY},
        source_name=source_name,
        slack_notifications=True,
    )


if __name__ == "__main__":
    generate_dataset()
