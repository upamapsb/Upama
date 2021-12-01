from datetime import date

import pandas as pd


def main() -> pd.DataFrame:

    print("Downloading South Africa data…")
    df = pd.read_csv(
        "https://raw.githubusercontent.com/dsfsi/covid19za/master/data/covid19za_timeline_testing.csv",
        usecols=["date", "hospitalisation"],
    )

    df["date"] = pd.to_datetime(df.date, dayfirst=True)

    df = (
        df[df.date >= pd.to_datetime("2021-06-21")]
        .set_index("date")
        .resample("1D")
        .asfreq()
        .reset_index()
        .sort_values("date")
    )
    df["hospitalisation"] = df.hospitalisation.interpolate("linear")
    df["hospitalisation"] = df.hospitalisation - df.hospitalisation.shift()

    # Hospital admissions
    df["date"] = (df.date + pd.to_timedelta(6 - df.date.dt.dayofweek, unit="d")).dt.date
    df = df[df.date <= date.today()]
    df = df.groupby("date", as_index=False).sum()
    df["date"] = pd.to_datetime(df.date)

    import pdb

    pdb.set_trace()

    # Merge
    swiss = swiss.melt("date", ["ICU_Covid19Patients", "Total_Covid19Patients", "entries"], "indicator")
    swiss.loc[:, "indicator"] = swiss["indicator"].replace(
        {
            "ICU_Covid19Patients": "Daily ICU occupancy",
            "Total_Covid19Patients": "Daily hospital occupancy",
            "entries": "Weekly new hospital admissions",
        },
    )

    swiss.loc[:, "entity"] = "Switzerland"
    swiss.loc[:, "iso_code"] = "CHE"
    swiss.loc[:, "population"] = 8715494

    return swiss
