import pandas as pd

SOURCE_URL = "https://lcps.nu/wp-content/uploads/covid-19-datafeed.csv"


def main() -> pd.DataFrame:

    print("Downloading Netherlands data…")
    df = pd.read_csv(
        SOURCE_URL,
        usecols=[
            "Datum",
            "Kliniek_Bedden_Nederland",
            "IC_Bedden_COVID_Nederland",
            "Kliniek_Nieuwe_Opnames_COVID_Nederland",
            "IC_Nieuwe_Opnames_COVID_Nederland",
        ],
    )

    df["Datum"] = pd.to_datetime(df.Datum, dayfirst=True).dt.date.astype(str)
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

    df["entity"] = "Netherlands"
    df["iso_code"] = "NLD"
    df["population"] = 17173094

    return df


if __name__ == "__main__":
    main()
