import pandas as pd
from datetime import date


def main():
    print("Downloading US data…")
    url = "https://healthdata.gov/api/views/g62h-syeh/rows.csv"

    usa = pd.read_csv(
        url,
        usecols=[
            "date",
            "total_adult_patients_hospitalized_confirmed_covid",
            "total_pediatric_patients_hospitalized_confirmed_covid",
            "staffed_icu_adult_patients_confirmed_covid",
            "previous_day_admission_adult_covid_confirmed",
            "previous_day_admission_pediatric_covid_confirmed",
        ],
    )

    usa.loc[:, "date"] = pd.to_datetime(usa.date)
    usa = usa[usa.date >= pd.to_datetime("2020-07-15")]
    usa = usa.groupby("date", as_index=False).sum()

    stock = usa[
        [
            "date",
            "total_adult_patients_hospitalized_confirmed_covid",
            "total_pediatric_patients_hospitalized_confirmed_covid",
            "staffed_icu_adult_patients_confirmed_covid",
        ]
    ].copy()
    stock.loc[:, "Daily hospital occupancy"] = stock.total_adult_patients_hospitalized_confirmed_covid.add(
        stock.total_pediatric_patients_hospitalized_confirmed_covid
    )
    stock = stock.rename(columns={"staffed_icu_adult_patients_confirmed_covid": "Daily ICU occupancy"})
    stock = stock[["date", "Daily hospital occupancy", "Daily ICU occupancy"]]
    stock = stock.melt(id_vars="date", var_name="indicator")
    stock.loc[:, "date"] = stock["date"].dt.date

    flow = usa[
        [
            "date",
            "previous_day_admission_adult_covid_confirmed",
            "previous_day_admission_pediatric_covid_confirmed",
        ]
    ].copy()
    flow.loc[:, "value"] = flow.previous_day_admission_adult_covid_confirmed.add(
        flow.previous_day_admission_pediatric_covid_confirmed
    )
    flow.loc[:, "date"] = (flow["date"] + pd.to_timedelta(6 - flow["date"].dt.dayofweek, unit="d")).dt.date
    flow = flow[flow["date"] <= date.today()]
    flow = flow[["date", "value"]]
    flow = flow.groupby("date", as_index=False).agg({"value": ["sum", "count"]})
    flow.columns = ["date", "value", "count"]
    flow = flow[flow["count"] == 7]
    flow = flow.drop(columns="count")
    flow.loc[:, "indicator"] = "Weekly new hospital admissions"

    # Merge all subframes
    usa = pd.concat([stock, flow])

    usa.loc[:, "entity"] = "United States"
    usa.loc[:, "iso_code"] = "USA"
    usa.loc[:, "population"] = 332915074

    return usa
