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
    stock["date"] = pd.to_datetime(stock["date"])

    # Hospital & ICU admissions
    flow = pd.read_csv(FLOW_URL, usecols=["jour", "incid_hosp", "incid_rea"], sep=";")
    flow = flow.rename(columns={"jour": "date"}).groupby("date", as_index=False).sum()
    flow["date"] = pd.to_datetime(flow["date"])
    flow["date"] = (flow["date"] + pd.to_timedelta(6 - flow["date"].dt.dayofweek, unit="d")).dt.date
    flow = flow[flow["date"] <= datetime.date.today()]
    flow = flow.groupby("date", as_index=False).agg({"incid_hosp": ["sum", "count"], "incid_rea": "sum"})
    flow.columns = ["date", "hosp_admissions", "count", "rea_admissions"]
    flow = flow[flow["count"] == 7]
    flow = flow.drop(columns="count")
    flow["date"] = pd.to_datetime(flow["date"])

    # Merge
    df = pd.merge(stock, flow, on="date", how="outer").melt(
        id_vars="date", value_vars=["hosp", "rea", "hosp_admissions", "rea_admissions"], var_name="indicator"
    )
    df.loc[:, "indicator"] = df["indicator"].replace(
        {
            "rea": "Daily ICU occupancy",
            "hosp": "Daily hospital occupancy",
            "hosp_admissions": "Weekly new hospital admissions",
            "rea_admissions": "Weekly new ICU admissions",
        },
    )

    df.loc[:, "entity"] = "France"
    df.loc[:, "iso_code"] = "FRA"
    df.loc[:, "population"] = 67564251

    return df


if __name__ == "__main__":
    main()
