import json

import pandas as pd
import requests


def main():
    print("Downloading Canada data…")
    url = "https://api.covid19tracker.ca/reports?after=2020-03-09"
    data = requests.get(url).json()
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

    df["date"] = df["date"].dt.date
    df["entity"] = "Canada"
    df["iso_code"] = "CAN"
    df["population"] = 38067913

    return df
