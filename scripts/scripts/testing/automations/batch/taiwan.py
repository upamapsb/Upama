from datetime import datetime

import pandas as pd


def read(source_url: str):
    return pd.read_csv(source_url, usecols=["通報日", "Total"])


def pipeline(df: pd.DataFrame, location: str) -> pd.DataFrame:
    # Filter
    df = df[~df.Total.isna() & df.Total > 0]
    # Rename columns
    df = df.rename(
        columns={
            "通報日": "Date",
            "Total": "Daily change in cumulative total",
        }
    )
    # Date
    date_str = df.Date.apply(lambda x: datetime.strptime(x, "%Y/%m/%d").strftime("%Y-%m-%d"))
    # Add columns
    df = df.assign(
        **{
            "Date": date_str,
            "Country": location,
            "Units": "people tested",
            "Source URL": "https://data.cdc.gov.tw/en/dataset/daily-cases-suspected-sars-cov-2-infection_tested",
            "Source label": "Taiwan CDC Open Data Portal",
            "Notes": pd.NA,
        }
    )
    return df[
        [
            "Date",
            "Daily change in cumulative total",
            "Notes",
            "Country",
            "Units",
            "Source URL",
            "Source label",
        ]
    ].sort_values("Date")


def main():
    source_url = "https://od.cdc.gov.tw/eic/covid19/covid19_tw_specimen.csv"
    location = "Taiwan"
    df = read(source_url).pipe(pipeline, location)
    df.to_csv(f"automated_sheets/{location}.csv", index=False)


if __name__ == "__main__":
    main()
