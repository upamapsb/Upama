import pandas as pd
from datetime import date


def main():
    print("Downloading UK data…")
    url = "https://api.coronavirus.data.gov.uk/v2/data?areaType=overview&metric=hospitalCases&metric=newAdmissions&metric=covidOccupiedMVBeds&format=csv"
    uk = pd.read_csv(url, usecols=["date", "hospitalCases", "newAdmissions", "covidOccupiedMVBeds"])
    uk.loc[:, "date"] = pd.to_datetime(uk["date"])

    stock = uk[["date", "hospitalCases", "covidOccupiedMVBeds"]].copy()
    stock = stock.melt("date", var_name="indicator")
    stock.loc[:, "date"] = stock["date"].dt.date

    flow = uk[["date", "newAdmissions"]].copy()
    flow.loc[:, "date"] = (flow["date"] + pd.to_timedelta(6 - flow["date"].dt.dayofweek, unit="d")).dt.date
    flow = flow[flow["date"] <= date.today()]
    flow = flow.groupby("date", as_index=False).agg({"newAdmissions": ["sum", "count"]})
    flow.columns = ["date", "newAdmissions", "count"]
    flow = flow[flow["count"] == 7]
    flow = flow.drop(columns="count").melt("date", var_name="indicator")

    uk = pd.concat([stock, flow]).dropna(subset=["value"])
    uk.loc[:, "indicator"] = uk["indicator"].replace(
        {
            "hospitalCases": "Daily hospital occupancy",
            "covidOccupiedMVBeds": "Daily ICU occupancy",
            "newAdmissions": "Weekly new hospital admissions",
        }
    )

    uk.loc[:, "entity"] = "United Kingdom"
    uk.loc[:, "iso_code"] = "GBR"
    uk.loc[:, "population"] = 68207114

    return uk
