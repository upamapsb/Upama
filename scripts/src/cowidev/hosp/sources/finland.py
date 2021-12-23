import requests

import pandas as pd

SOURCE_URL = "https://w3qa5ydb4l.execute-api.eu-west-1.amazonaws.com/prod/finnishCoronaHospitalData"


def main() -> pd.DataFrame:

    print("Downloading Finland data…")

    data = requests.get(SOURCE_URL).json()
    df = pd.DataFrame.from_records(data["hospitalised"])

    df = df[df.area == "Finland"][["date", "totalHospitalised", "inIcu"]]
    df["date"] = df.date.astype(str).str.slice(0, 10)

    df = df.melt("date", var_name="indicator").dropna(subset=["value"])
    df["indicator"] = df.indicator.replace(
        {
            "totalHospitalised": "Daily hospital occupancy",
            "inIcu": "Daily ICU occupancy",
        }
    )

    df["entity"] = "Finland"
    df["iso_code"] = "FIN"
    df["population"] = 5548361

    return df


if __name__ == "__main__":
    main()
