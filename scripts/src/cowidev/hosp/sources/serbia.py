import pandas as pd

METADATA = {
    "source_url": "https://raw.githubusercontent.com/aleksandar-jovicic/COVID19-Serbia/master/timeseries.csv",
    "source_url_ref": "https://github.com/aleksandar-jovicic/COVID19-Serbia",
    "source_name": "Ministry of Health via github.com/aleksandar-jovicic/COVID19-Serbia",
    "entity": "Serbia",
}


def main():
    df = (
        pd.read_csv(METADATA["source_url"], usecols=["ts", "hospitalized", "ventilated"])
        .rename(
            columns={"ts": "date", "hospitalized": "Daily hospital occupancy", "ventilated": "Daily ICU occupancy"}
        )
        .melt(id_vars="date", var_name="indicator", value_name="value")
        .assign(
            entity=METADATA["entity"],
        )
    )
    df["date"] = df.date.str.slice(0, 10)
    return df, METADATA


if __name__ == "__main__":
    main()
