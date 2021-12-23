import pandas as pd


def main():
    print("Downloading Serbia data…")

    url = "https://raw.githubusercontent.com/aleksandar-jovicic/COVID19-Serbia/master/timeseries.csv"
    df = (
        pd.read_csv(url, usecols=["ts", "hospitalized", "ventilated"])
        .rename(
            columns={"ts": "date", "hospitalized": "Daily hospital occupancy", "ventilated": "Daily ICU occupancy"}
        )
        .melt(id_vars="date", var_name="indicator", value_name="value")
        .assign(
            entity="Serbia",
            iso_code="SRB",
            population=6908224,
        )
    )
    df["date"] = df.date.str.slice(0, 10)
    return df


if __name__ == "__main__":
    main()
