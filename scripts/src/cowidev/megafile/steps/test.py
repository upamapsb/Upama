import os
from datetime import date

import pandas as pd
from cowidev.utils.utils import get_project_dir


INPUT_DIR = os.path.abspath(os.path.join(get_project_dir(), "scripts", "input"))
DATA_DIR = os.path.abspath(os.path.join(get_project_dir(), "public", "data"))
data_file = os.path.join(DATA_DIR, "testing", "covid-testing-all-observations.csv")
data_file_second = os.path.join(INPUT_DIR, "owid", "secondary_testing_series.csv")


def get_testing():
    """
    Reads the main COVID-19 testing dataset located in /public/data/testing/
    Rearranges the Entity column to separate location from testing units
    Checks for duplicated location/date couples, as we can have more than 1 time series per country

    Returns:
        testing {dataframe}
    """

    testing = pd.read_csv(
        data_file,
        usecols=[
            "Entity",
            "Date",
            "Cumulative total",
            "Daily change in cumulative total",
            "7-day smoothed daily change",
            "Cumulative total per thousand",
            "Daily change in cumulative total per thousand",
            "7-day smoothed daily change per thousand",
            "Short-term positive rate",
            "Short-term tests per case",
        ],
    )

    testing = testing.rename(
        columns={
            "Entity": "location",
            "Date": "date",
            "Cumulative total": "total_tests",
            "Daily change in cumulative total": "new_tests",
            "7-day smoothed daily change": "new_tests_smoothed",
            "Cumulative total per thousand": "total_tests_per_thousand",
            "Daily change in cumulative total per thousand": "new_tests_per_thousand",
            "7-day smoothed daily change per thousand": "new_tests_smoothed_per_thousand",
            "Short-term positive rate": "positive_rate",
            "Short-term tests per case": "tests_per_case",
        }
    )

    testing[
        [
            "total_tests_per_thousand",
            "new_tests_per_thousand",
            "new_tests_smoothed_per_thousand",
            "tests_per_case",
        ]
    ] = testing[
        [
            "total_tests_per_thousand",
            "new_tests_per_thousand",
            "new_tests_smoothed_per_thousand",
            "tests_per_case",
        ]
    ].round(
        3
    )

    # Split the original entity into location and testing units
    testing[["location", "tests_units"]] = testing.location.str.split(" - ", expand=True)

    # For locations with >1 series, choose a series
    to_remove = pd.read_csv(data_file_second)
    for loc, unit in to_remove.itertuples(index=False, name=None):
        testing = testing[-((testing["location"] == loc) & (testing["tests_units"] == unit))]

    # Check for remaining duplicates of location/date
    duplicates = testing.groupby(["location", "date"]).size().to_frame("n")
    duplicates = duplicates[duplicates["n"] > 1]
    if duplicates.shape[0] > 0:
        print(duplicates)
        raise Exception("Multiple rows for the same location and date")

    # Remove observations for current day to avoid rows with testing data but no case/deaths
    testing = testing[testing["date"] < str(date.today())]

    return testing
