import pandas as pd

from cowidev.utils.clean import clean_date_series


METADATA = {
    "source_url": "https://lcps.nu/wp-content/uploads/covid-19-datafeed.csv",
    "source_url_ref": "https://lcps.nu/datafeed/",
    "source_name": "National Coordination Center Patient Distribution",
    "entity": "Netherlands",
}


def main() -> pd.DataFrame:
    df = pd.read_csv(
        METADATA["source_url"],
        usecols=[
            "Datum",
            "Kliniek_Bedden_Nederland",
            "IC_Bedden_COVID_Nederland",
            "Kliniek_Nieuwe_Opnames_COVID_Nederland",
            "IC_Nieuwe_Opnames_COVID_Nederland",
        ],
    )
    df["Datum"] = clean_date_series(df.Datum, "%d-%m-%Y")
    df = df.rename(columns={"Datum": "date"}).sort_values("date")

    df["Kliniek_Nieuwe_Opnames_COVID_Nederland"] = df.Kliniek_Nieuwe_Opnames_COVID_Nederland.rolling(7).sum()
    df["IC_Nieuwe_Opnames_COVID_Nederland"] = df.IC_Nieuwe_Opnames_COVID_Nederland.rolling(7).sum()

    df = df.melt("date", var_name="indicator").dropna(subset=["value"])
    df["indicator"] = df.indicator.replace(
        {
            "Kliniek_Bedden_Nederland": "Daily hospital occupancy",
            "IC_Bedden_COVID_Nederland": "Daily ICU occupancy",
            "Kliniek_Nieuwe_Opnames_COVID_Nederland": "Weekly new hospital admissions",
            "IC_Nieuwe_Opnames_COVID_Nederland": "Weekly new ICU admissions",
        }
    )

    df["entity"] = METADATA["entity"]

    return df, METADATA


if __name__ == "__main__":
    main()
