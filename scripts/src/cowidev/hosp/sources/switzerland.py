import pandas as pd
import requests


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

    # Hospital admissions
    url = context["sources"]["individual"]["csv"]["daily"]["hosp"]
    flow = pd.read_csv(url, usecols=["datum", "geoRegion", "entries"])
    flow = (
        flow[flow.geoRegion == "CH"].drop(columns=["geoRegion"]).rename(columns={"datum": "date"}).sort_values("date")
    )
    flow["entries"] = flow.entries.rolling(7).sum()

    # Merge
    df = pd.merge(stock, flow, on="date", how="outer")
    df = df.melt("date", var_name="indicator").dropna(subset=["value"])
    df["indicator"] = df.indicator.replace(
        {
            "ICU_Covid19Patients": "Daily ICU occupancy",
            "Total_Covid19Patients": "Daily hospital occupancy",
            "entries": "Weekly new hospital admissions",
        },
    )

    df["entity"] = "Switzerland"
    df["iso_code"] = "CHE"
    df["population"] = 8715494

    return df


if __name__ == "__main__":
    main()
