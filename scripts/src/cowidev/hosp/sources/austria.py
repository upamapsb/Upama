import io
import os
import requests
import tempfile
import zipfile

import pandas as pd

from cowidev.utils.clean import clean_date_series

METADATA = {
    "source_url": "https://covid19-dashboard.ages.at/data/data.zip",
    "source_url_ref": "https://covid19-dashboard.ages.at/",
    "source_name": "Austrian Agency for Health and Food Safety",
    "entity": "Austria",
}


def read() -> pd.DataFrame:
    with tempfile.TemporaryDirectory() as tf:
        r = requests.get(METADATA["source_url"])
        z = zipfile.ZipFile(io.BytesIO(r.content))
        z.extractall(tf)
        df = pd.read_csv(
            os.path.join(tf, "CovidFallzahlen.csv"),
            sep=";",
            usecols=["Meldedat", "Bundesland", "FZHosp", "FZICU"],
        )
    return df


def main() -> pd.DataFrame:
    df = read()

    df = df[df.Bundesland == "Alle"].drop(columns="Bundesland").rename(columns={"Meldedat": "date"})
    df["date"] = clean_date_series(df.date, "%d.%m.%Y")

    df = df.melt("date", var_name="indicator").dropna(subset=["value"])
    df["indicator"] = df.indicator.replace(
        {
            "FZHosp": "Daily hospital occupancy",
            "FZICU": "Daily ICU occupancy",
        },
    )

    df["entity"] = METADATA["entity"]

    return df, METADATA


if __name__ == "__main__":
    main()
