import pandas as pd

METADATA = {
    "source_url": "https://api.coronavirus.data.gov.uk/v2/data?areaType=overview&metric=hospitalCases&metric=newAdmissions&metric=covidOccupiedMVBeds&format=csv",
    "source_url_ref": "https://coronavirus.data.gov.uk/details/healthcare",
    "source_name": "Government of the United Kingdom",
    "entity": "United Kingdom",
}


def main():
    df = pd.read_csv(METADATA["source_url"], usecols=["date", "hospitalCases", "newAdmissions", "covidOccupiedMVBeds"])

    df = df.sort_values("date")

    df["newAdmissions"] = df.newAdmissions.rolling(7).sum()

    df = df.melt("date", var_name="indicator").dropna(subset=["value"])
    df["indicator"] = df.indicator.replace(
        {
            "hospitalCases": "Daily hospital occupancy",
            "covidOccupiedMVBeds": "Daily ICU occupancy",
            "newAdmissions": "Weekly new hospital admissions",
        }
    )

    df["entity"] = METADATA["entity"]

    return df, METADATA


if __name__ == "__main__":
    main()
