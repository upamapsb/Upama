import pandas as pd


METADATA = {
    "source_url": "https://raw.githubusercontent.com/dancarmoz/israel_moh_covid_dashboard_data/master/hospitalized_and_infected.csv",
    "source_url_ref": "https://datadashboard.health.gov.il/COVID-19/",
    "source_name": "Ministry of Health, via dancarmoz on GitHub",
    "entity": "Israel",
}


def main():
    df = (
        pd.read_csv(
            METADATA["source_url"], usecols=["Date", "Hospitalized", "Critical", "New hosptialized", "New serious"]
        )
        .rename(columns={"Date": "date"})
        .sort_values("date")
    )

    df["New hosptialized"] = df["New hosptialized"].rolling(7).sum()
    df["New serious"] = df["New serious"].rolling(7).sum()

    df = df.melt("date", var_name="indicator").dropna(subset=["value"])
    df["indicator"] = df.indicator.replace(
        {
            "Hospitalized": "Daily hospital occupancy",
            "Critical": "Daily ICU occupancy",
            "New hosptialized": "Weekly new hospital admissions",
            "New serious": "Weekly new ICU admissions",
        }
    )

    df["entity"] = METADATA["entity"]

    return df, METADATA


if __name__ == "__main__":
    main()
