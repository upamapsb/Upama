import pandas as pd


METADATA = {
    "source_url_stock": "https://github.com/erasta/CovidDataIsrael/raw/master/out/csv/hospitalizationStatusDaily.csv",
    "source_url_flow": "https://github.com/erasta/CovidDataIsrael/raw/master/out/csv/patientsPerDate.csv",
    "source_url_ref": "https://datadashboard.health.gov.il/COVID-19/",
    "source_name": "Ministry of Health, via Eran Geva on GitHub",
    "entity": "Israel",
}


def main():

    stock = pd.read_csv(METADATA["source_url_stock"]).rename(columns={"dayDate": "date"})
    stock["hospital_stock"] = stock.countHardStatus + stock.countMediumStatus + stock.countEasyStatus
    stock = stock.drop(columns=["countMediumStatus", "countEasyStatus"])

    flow = pd.read_csv(METADATA["source_url_flow"], usecols=["date", "serious_critical_new"]).sort_values("date")
    flow["serious_critical_new"] = flow.serious_critical_new.rolling(7).sum()

    df = (
        pd.merge(stock, flow, on="date", how="outer", validate="one_to_one")
        .melt("date", var_name="indicator")
        .dropna(subset=["value"])
        .sort_values("date")
        .head(-1)
    )
    df["indicator"] = df.indicator.replace(
        {
            "hospital_stock": "Daily hospital occupancy",
            "countHardStatus": "Daily ICU occupancy",
            "serious_critical_new": "Weekly new ICU admissions",
        }
    )

    df["date"] = df.date.str.slice(0, 10)
    df["entity"] = METADATA["entity"]

    return df, METADATA


if __name__ == "__main__":
    main()
