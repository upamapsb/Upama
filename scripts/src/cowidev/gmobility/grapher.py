import os
from datetime import datetime
import pandas as pd

from cowidev.grapher.db.base import GrapherBaseUpdater
from cowidev.utils.utils import time_str_grapher, get_filename
from cowidev.utils.clean.dates import DATE_FORMAT
from cowidev.gmobility.dtypes import dtype

ZERO_DAY = "2020-01-01"
zero_day = datetime.strptime(ZERO_DAY, DATE_FORMAT)


def run_grapheriser(input_path: str, input_path_country_std: str, output_path: str):
    mobility = pd.read_csv(input_path, dtype=dtype)

    # Convert date column to days since zero_day
    mobility["date"] = pd.to_datetime(mobility["date"], format="%Y/%m/%d").map(lambda date: (date - zero_day).days)

    # Standardise country names to OWID country names
    country_mapping = pd.read_csv(input_path_country_std)
    mobility = country_mapping.merge(mobility, on="country_region")

    # Remove subnational data, keeping only country figures
    filter_cols = [
        "sub_region_1",
        "sub_region_2",
        "metro_area",
        "iso_3166_2_code",
        "census_fips_code",
    ]
    country_mobility = mobility[mobility[filter_cols].isna().all(1)]

    # Delete columns
    country_mobility = country_mobility.drop(
        columns=[
            "country_region",
            "sub_region_1",
            "sub_region_2",
            "metro_area",
            "census_fips_code",
            "iso_3166_2_code",
        ]
    )

    # Assign new column names
    rename_dict = {
        "date": "Year",
        "retail_and_recreation_percent_change_from_baseline": "retail_and_recreation",
        "grocery_and_pharmacy_percent_change_from_baseline": "grocery_and_pharmacy",
        "parks_percent_change_from_baseline": "parks",
        "transit_stations_percent_change_from_baseline": "transit_stations",
        "workplaces_percent_change_from_baseline": "workplaces",
        "residential_percent_change_from_baseline": "residential",
    }

    # Rename columns
    country_mobility = country_mobility.rename(columns=rename_dict)

    # Replace time series with 7-day rolling averages
    country_mobility = country_mobility.sort_values(by=["Country", "Year"]).reset_index(drop=True)
    smoothed_cols = [
        "retail_and_recreation",
        "grocery_and_pharmacy",
        "parks",
        "transit_stations",
        "workplaces",
        "residential",
    ]
    country_mobility[smoothed_cols] = (
        country_mobility.groupby("Country", as_index=False)
        .rolling(window=7, min_periods=3, center=False)
        .mean()
        .round(3)
        .reset_index()[smoothed_cols]
    )

    # Save to files
    country_mobility.to_csv(output_path, index=False)

    os.remove(input_path)


def run_db_updater(input_path: str):
    dataset_name = get_filename(input_path)
    GrapherBaseUpdater(
        dataset_name=dataset_name,
        source_name=f"Google COVID-19 Community Mobility Trends – Last updated {time_str_grapher()} (London time)",
        zero_day=ZERO_DAY,
        slack_notifications=False,
    ).run()
