import pandas as pd


def main():

    print("Downloading Algeria data…")
    url = "https://raw.githubusercontent.com/yasserkaddour/covid19-icu-data-algeria/main/algeria-covid19-icu-data.csv"
    df = pd.read_csv(url, usecols=["date", "in_icu"])

    df = df.melt("date", ["in_icu"], "indicator")
    df["indicator"] = df.indicator.replace({"in_icu": "Daily ICU occupancy"})

    df["entity"] = "Algeria"
    df["iso_code"] = "DZA"
    df["population"] = 44616626

    return df
