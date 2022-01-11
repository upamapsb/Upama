import json
import os
import sys
import requests

import pandas as pd

CURRENT_DIR = os.path.dirname(__file__)
sys.path.append(CURRENT_DIR)

from utils.db_imports import import_dataset

DATASET_NAME = "COVID-19 - Decoupling of metrics"
GRAPHER_PATH = os.path.join(CURRENT_DIR, "../grapher/")
ZERO_DAY = "2020-01-01"
SOURCE_SPAIN = "https://cnecovid.isciii.es/covid19/resources/casos_hosp_uci_def_sexo_edad_provres.csv"
SOURCE_ISRAEL = "https://github.com/dancarmoz/israel_moh_covid_dashboard_data/raw/master/hospitalized_and_infected.csv"


def adjust_x_and_y(
    df: pd.DataFrame,
    start_date: str,
    case_peak_date: str,
    hosp_peak_date: str,
    icu_peak_date: str,
    death_peak_date: str,
    hosp_variable: str,
    icu_variable: str,
) -> pd.DataFrame:
    df = df[df.date >= start_date].copy()

    case_peak = df.loc[df.date == case_peak_date, "confirmed_cases"].values[0]
    hosp_peak = df.loc[df.date == hosp_peak_date, hosp_variable].values[0]
    icu_peak = df.loc[df.date == icu_peak_date, icu_variable].values[0]
    death_peak = df.loc[df.date == death_peak_date, "confirmed_deaths"].values[0]

    hosp_shift = (pd.to_datetime(hosp_peak_date) - pd.to_datetime(case_peak_date)).days
    icu_shift = (pd.to_datetime(icu_peak_date) - pd.to_datetime(case_peak_date)).days
    death_shift = (pd.to_datetime(death_peak_date) - pd.to_datetime(case_peak_date)).days

    df[hosp_variable] = df[hosp_variable].shift(-hosp_shift)
    df[icu_variable] = df[icu_variable].shift(-icu_shift)
    df["confirmed_deaths"] = df.confirmed_deaths.shift(-death_shift)

    df["confirmed_cases"] = 100 * df.confirmed_cases / case_peak
    df[hosp_variable] = 100 * df[hosp_variable] / hosp_peak
    df[icu_variable] = 100 * df[icu_variable] / icu_peak
    df["confirmed_deaths"] = 100 * df.confirmed_deaths / death_peak

    return df


def process_esp() -> pd.DataFrame:

    df = (
        pd.read_csv(SOURCE_SPAIN, usecols=["fecha", "num_casos", "num_hosp", "num_uci", "num_def"])
        .rename(
            columns={
                "fecha": "date",
                "num_def": "confirmed_deaths",
                "num_casos": "confirmed_cases",
                "num_hosp": "hospital_flow",
                "num_uci": "icu_flow",
            }
        )
        .groupby("date", as_index=False)
        .sum()
        .assign(Country="Spain")
        .sort_values("date")
        .head(-7)
    )

    df[["confirmed_cases", "confirmed_deaths", "hospital_flow", "icu_flow"]] = (
        df[["confirmed_cases", "confirmed_deaths", "hospital_flow", "icu_flow"]].rolling(7).sum()
    )

    df = adjust_x_and_y(
        df,
        start_date="2020-12-15",
        case_peak_date="2021-01-21",
        hosp_peak_date="2021-01-24",
        icu_peak_date="2021-01-24",
        death_peak_date="2021-01-30",
        hosp_variable="hospital_flow",
        icu_variable="icu_flow",
    )

    return df


def process_isr() -> pd.DataFrame:

    df = (
        pd.read_csv(SOURCE_ISRAEL, usecols=["Date", "New infected", "New hosptialized", "New serious", "New deaths"])
        .rename(
            columns={
                "Date": "date",
                "New infected": "confirmed_cases",
                "New hosptialized": "hospital_flow",
                "New serious": "icu_flow",
                "New deaths": "confirmed_deaths",
            }
        )
        .sort_values("date")
        .assign(Country="Israel")
        .head(-1)
    )

    vars = ["confirmed_cases", "hospital_flow", "icu_flow", "confirmed_deaths"]
    df[vars] = df[vars].rolling(7).sum()

    df = adjust_x_and_y(
        df,
        start_date="2020-11-15",
        case_peak_date="2021-01-15",
        hosp_peak_date="2021-01-17",
        icu_peak_date="2021-01-19",
        death_peak_date="2021-01-28",
        hosp_variable="hospital_flow",
        icu_variable="icu_flow",
    )

    return df


def main():
    spain = process_esp()
    israel = process_isr()
    df = pd.concat([spain, israel], ignore_index=True).rename(columns={"date": "Year"})
    df["Year"] = (pd.to_datetime(df.Year) - pd.to_datetime(ZERO_DAY)).dt.days
    df = df[["Country", "Year", "confirmed_cases", "hospital_flow", "icu_flow", "confirmed_deaths"]]
    df.to_csv(os.path.join(GRAPHER_PATH, f"{DATASET_NAME}.csv"), index=False)


def update_db():
    import_dataset(
        dataset_name=DATASET_NAME,
        namespace="owid",
        csv_path=os.path.join(GRAPHER_PATH, DATASET_NAME + ".csv"),
        default_variable_display={"yearIsDay": True, "zeroDay": ZERO_DAY},
        source_name="Official data collated by Our World in Data",
        slack_notifications=True,
    )


if __name__ == "__main__":
    main()
