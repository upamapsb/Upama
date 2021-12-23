import io
import os
import requests
import tempfile
import zipfile

import pandas as pd

SOURCE_URL = "https://covid19-dashboard.ages.at/data/data.zip"


def read() -> pd.DataFrame:
    with tempfile.TemporaryDirectory() as tf:
        r = requests.get(SOURCE_URL)
        z = zipfile.ZipFile(io.BytesIO(r.content))
        z.extractall(tf)
        df = pd.read_csv(
            os.path.join(tf, "CovidFallzahlen.csv"),
            sep=";",
            usecols=["Meldedat", "Bundesland", "FZHosp", "FZICU"],
        )
    return df


def main() -> pd.DataFrame:

    print("Downloading Austria data…")
    df = read()

    df = df[df.Bundesland == "Alle"].drop(columns="Bundesland").rename(columns={"Meldedat": "date"})
    df["date"] = pd.to_datetime(df.date, dayfirst=True).dt.date.astype(str)

    df = df.melt("date", var_name="indicator").dropna(subset=["value"])
    df["indicator"] = df.indicator.replace(
        {
            "FZHosp": "Daily hospital occupancy",
            "FZICU": "Daily ICU occupancy",
        },
    )

    df["entity"] = "Austria"

    return df


if __name__ == "__main__":
    main()
