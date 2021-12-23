import io
import os
import requests
import tempfile
import zipfile

import pandas as pd

from cowidev.utils.web.scraping import get_soup

SOURCE_URL = "https://covid19.ssi.dk/overvagningsdata/download-fil-med-overvaagningdata"


def read() -> pd.DataFrame:
    soup = get_soup(SOURCE_URL)
    zip_url = soup.find("accordions").find("a").get("href")

    with tempfile.TemporaryDirectory() as tf:
        r = requests.get(zip_url)
        z = zipfile.ZipFile(io.BytesIO(r.content))
        z.extractall(tf)
        flow = pd.read_csv(
            os.path.join(tf, "Regionalt_DB", "06_nye_indlaeggelser_pr_region_pr_dag.csv"),
            encoding="ISO 8859-1",
            sep=";",
            usecols=["Dato", "Indlæggelser"],
        )
        stock = pd.read_csv(
            os.path.join(tf, "Regionalt_DB", "15_indlagte_pr_region_pr_dag.csv"),
            encoding="ISO 8859-1",
            sep=";",
            usecols=["Dato", "Indlagte"],
        )
    return flow, stock


def main() -> pd.DataFrame:

    print("Downloading Denmark data…")
    flow, stock = read()

    flow = flow.rename(columns={"Dato": "date"}).groupby("date", as_index=False).sum().sort_values("date")
    flow["Indlæggelser"] = flow.Indlæggelser.rolling(7).sum()

    stock = stock.rename(columns={"Dato": "date"}).groupby("date", as_index=False).sum()

    df = pd.merge(flow, stock, how="outer", on="date", validate="one_to_one")

    df = df.melt("date", var_name="indicator").dropna(subset=["value"])
    df["indicator"] = df.indicator.replace(
        {
            "Indlæggelser": "Weekly new hospital admissions",
            "Indlagte": "Daily hospital occupancy",
        },
    )

    df["entity"] = "Denmark"
    df["iso_code"] = "DNK"
    df["population"] = 5813302

    return df


if __name__ == "__main__":
    main()
