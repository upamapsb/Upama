import pandas as pd


def main():

    print("Downloading Algeria data…")
    url = "https://raw.githubusercontent.com/yasserkaddour/covid19-icu-data-algeria/main/algeria-covid19-icu-data.csv"
    algeria = pd.read_csv(url, usecols=["date", "in_icu"])

    algeria = algeria.melt("date", ["in_icu"], "indicator")
    algeria.loc[:, "indicator"] = algeria["indicator"].replace({"in_icu": "Daily ICU occupancy"})

    algeria.loc[:, "entity"] = "Algeria"
    algeria.loc[:, "iso_code"] = "DZA"
    algeria.loc[:, "population"] = 44616626

    return algeria
