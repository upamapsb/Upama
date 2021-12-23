import pandas as pd


METADATA = {
    "source_url": (
        "https://raw.githubusercontent.com/yasserkaddour/covid19-icu-data-algeria/main/algeria-covid19-icu-data.csv"
    ),
    "entity": "Algeria",
}


def main():
    print("Downloading Algeria data…")
    df = pd.read_csv(METADATA["source_url"], usecols=["date", "in_icu"])
    df = df.melt("date", ["in_icu"], "indicator")
    df = df.assign(
        indicator=df.indicator.replace({"in_icu": "Daily ICU occupancy"}),
        entity=METADATA["entity"],
    )
    return df
