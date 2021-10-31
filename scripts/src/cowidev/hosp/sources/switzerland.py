import pandas as pd
import requests
from datetime import date


def main() -> pd.DataFrame:

    print("Downloading Switzerland data…")
    context = requests.get("https://www.covid19.admin.ch/api/data/context").json()

    # Hospital & ICU patients
    url = context["sources"]["individual"]["csv"]["daily"]["hospCapacity"]
    stock = pd.read_csv(
        url,
        usecols=[
            "date",
            "geoRegion",
            "type_variant",
            "ICU_Covid19Patients",
            "Total_Covid19Patients",
        ],
    )
    stock = stock[(stock.geoRegion == "CH") & (stock.type_variant == "fp7d")].drop(
        columns=["geoRegion", "type_variant"]
    )
    stock.loc[:, "date"] = pd.to_datetime(stock["date"])

    # Hospital admissions
    url = context["sources"]["individual"]["csv"]["daily"]["hosp"]
    flow = pd.read_csv(url, usecols=["datum", "geoRegion", "entries"])
    flow = flow[flow.geoRegion == "CH"].drop(columns=["geoRegion"]).rename(columns={"datum": "date"})
    flow.loc[:, "date"] = pd.to_datetime(flow["date"])
    flow.loc[:, "date"] = (flow["date"] + pd.to_timedelta(6 - flow["date"].dt.dayofweek, unit="d")).dt.date
    flow = flow[flow["date"] <= date.today()]
    flow = flow.groupby("date", as_index=False).agg({"entries": ["sum", "count"]})
    flow.columns = ["date", "entries", "count"]
    flow = flow[flow["count"] == 7]
    flow = flow.drop(columns="count")
    flow.loc[:, "date"] = pd.to_datetime(flow["date"])

    # Merge
    swiss = pd.merge(stock, flow, on="date", how="outer")
    swiss = swiss.melt("date", ["ICU_Covid19Patients", "Total_Covid19Patients", "entries"], "indicator")
    swiss.loc[:, "indicator"] = swiss["indicator"].replace(
        {
            "ICU_Covid19Patients": "Daily ICU occupancy",
            "Total_Covid19Patients": "Daily hospital occupancy",
            "entries": "Weekly new hospital admissions",
        },
    )

    swiss.loc[:, "entity"] = "Switzerland"
    swiss.loc[:, "iso_code"] = "CHE"
    swiss.loc[:, "population"] = 8715494

    return swiss
