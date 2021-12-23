import datetime

import pandas as pd


STOCK_URL = "https://www.data.gouv.fr/fr/datasets/r/63352e38-d353-4b54-bfd1-f1b3ee1cabd7"
FLOW_URL = "https://www.data.gouv.fr/fr/datasets/r/6fadff46-9efd-4c53-942a-54aca783c30c"


def main() -> pd.DataFrame:

    print("Downloading France data…")

    # Hospital & ICU patients
    stock = pd.read_csv(STOCK_URL, usecols=["sexe", "jour", "hosp", "rea"], sep=";")
    stock = (
        stock[stock.sexe == 0]
        .drop(columns=["sexe"])
        .rename(columns={"jour": "date"})
        .groupby("date", as_index=False)
        .sum()
    )

    # Hospital & ICU admissions
    flow = pd.read_csv(FLOW_URL, usecols=["jour", "incid_hosp", "incid_rea"], sep=";")
    flow = flow.rename(columns={"jour": "date"}).groupby("date", as_index=False).sum().sort_values("date")
    flow["incid_hosp"] = flow.incid_hosp.rolling(7).sum()
    flow["incid_rea"] = flow.incid_rea.rolling(7).sum()

    # Merge
    df = pd.merge(stock, flow, on="date", how="outer").melt("date", var_name="indicator").dropna(subset=["value"])
    df["indicator"] = df.indicator.replace(
        {
            "rea": "Daily ICU occupancy",
            "hosp": "Daily hospital occupancy",
            "incid_hosp": "Weekly new hospital admissions",
            "incid_rea": "Weekly new ICU admissions",
        },
    )

    df["entity"] = "France"

    return df


if __name__ == "__main__":
    main()
