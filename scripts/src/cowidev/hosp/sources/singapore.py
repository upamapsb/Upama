import requests

import pandas as pd

METADATA = {
    "source_url": "https://covidsitrep.moh.gov.sg/_dash-layout",
    "source_url_ref": "https://covidsitrep.moh.gov.sg/",
    "source_name": "Ministry of Health",
    "entity": "Singapore",
}


def main():

    data = requests.get(METADATA["source_url"]).json()

    charts = data["props"]["children"][1]["props"]["children"][3]["props"]["children"][0]["props"]["figure"]["data"]

    hospital_chart = charts[3]
    hosp = pd.DataFrame({"date": hospital_chart["x"], "hospital_stock": hospital_chart["y"]})

    icu_chart = charts[6]
    icu = pd.DataFrame({"date": icu_chart["x"], "icu_stock": icu_chart["y"]})

    df = (
        pd.merge(hosp, icu, on="date", how="outer", validate="one_to_one")
        .melt("date", var_name="indicator")
        .dropna(subset=["value"])
    )
    df["indicator"] = df.indicator.replace(
        {
            "hospital_stock": "Daily hospital occupancy",
            "icu_stock": "Daily ICU occupancy",
        }
    )

    df["entity"] = METADATA["entity"]
    df["date"] = df.date.str.slice(0, 10)

    return df, METADATA


if __name__ == "__main__":
    main()
