import json

import pandas as pd
import requests

from cowidev.utils.clean import clean_date_series

METADATA = {
    "source_url": "https://api.covid19tracker.ca/reports?after=2020-03-09",
    "source_url_ref": "https://covid19tracker.ca/",
    "source_name": "Official data from provinces via covid19tracker.ca",
    "entity": "Canada",
}


def main():
    data = requests.get(METADATA["source_url"]).json()
    data = json.dumps(data["data"])

    df = pd.read_json(data, orient="records")
    df = df[["date", "total_hospitalizations", "total_criticals"]]

    df = df.melt("date", ["total_hospitalizations", "total_criticals"], "indicator")
    df["indicator"] = df.indicator.replace(
        {
            "total_hospitalizations": "Daily hospital occupancy",
            "total_criticals": "Daily ICU occupancy",
        }
    )
    df["date"] = clean_date_series(df.date, "%Y.%m.%d")
    df["entity"] = METADATA["entity"]

    return df, METADATA
