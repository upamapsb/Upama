import pandas as pd


def main():
    print("Downloading UK data…")

    url = "https://api.coronavirus.data.gov.uk/v2/data?areaType=overview&metric=hospitalCases&metric=newAdmissions&metric=covidOccupiedMVBeds&format=csv"
    df = pd.read_csv(url, usecols=["date", "hospitalCases", "newAdmissions", "covidOccupiedMVBeds"])

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

    df["entity"] = "United Kingdom"
    df["iso_code"] = "GBR"
    df["population"] = 68207114

    return df


if __name__ == "__main__":
    main()
