import json

import pandas as pd
import requests


def main():
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

    return canada
