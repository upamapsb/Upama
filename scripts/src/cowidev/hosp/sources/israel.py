import pandas as pd
import numpy as np

from cowidev.utils.clean import clean_date_series


METADATA = {
    "source_url": "https://datadashboardapi.health.gov.il/api/queries/patientsPerDate",
    "source_url_ref": "https://datadashboard.health.gov.il/COVID-19/",
    "source_name": "Ministry of Health",
    "entity": "Israel",
}


def main():
    url = "https://datadashboardapi.health.gov.il/api/queries/patientsPerDate"
    df = pd.read_json(url)

    df["date"] = clean_date_series(df.date, "%Y-%m-%dT%H:%M:%S.%fZ")
    df = df.sort_values("date")

    df.loc[df.date < "2020-08-17", "CountCriticalStatus"] = np.nan

    df["new_hospitalized"] = df.new_hospitalized.rolling(7).sum()
    df["serious_critical_new"] = df.new_hospitalized.rolling(7).sum()

    df = (
        df[["date", "Counthospitalized", "CountCriticalStatus", "new_hospitalized", "serious_critical_new"]]
        .melt("date", var_name="indicator")
        .dropna(subset=["value"])
    )
    df["indicator"] = df.indicator.replace(
        {
            "Counthospitalized": "Daily hospital occupancy",
            "CountCriticalStatus": "Daily ICU occupancy",
            "new_hospitalized": "Weekly new hospital admissions",
            "serious_critical_new": "Weekly new ICU admissions",
        }
    )

    df["entity"] = METADATA["entity"]

    return df, METADATA


if __name__ == "__main__":
    main()
