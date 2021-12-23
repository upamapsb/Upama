import pandas as pd

SOURCE_URL = "https://epistat.sciensano.be/Data/COVID19BE_HOSP.csv"


def main() -> pd.DataFrame:

    print("Downloading Belgium data…")
    df = pd.read_csv(SOURCE_URL, usecols=["DATE", "TOTAL_IN", "TOTAL_IN_ICU", "NEW_IN"])

    df = df.rename(columns={"DATE": "date"}).groupby("date", as_index=False).sum().sort_values("date")

    df["NEW_IN"] = df.NEW_IN.rolling(7).sum()

    df = df.melt("date", var_name="indicator").dropna(subset=["value"])
    df["indicator"] = df.indicator.replace(
        {
            "TOTAL_IN": "Daily hospital occupancy",
            "TOTAL_IN_ICU": "Daily ICU occupancy",
            "NEW_IN": "Weekly new hospital admissions",
        }
    )

    df["entity"] = "Belgium"
    df["iso_code"] = "BEL"
    df["population"] = 11632334

    return df


if __name__ == "__main__":
    main()
