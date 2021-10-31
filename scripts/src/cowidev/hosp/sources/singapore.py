import datetime

import pandas as pd
import requests


def main():
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
    return singapore
