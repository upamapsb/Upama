import pandas as pd


def main():

    print("Downloading Malaysia data…")

    url = "https://raw.githubusercontent.com/MoH-Malaysia/covid19-public/main/epidemic/hospital.csv"
    hosp = pd.read_csv(url, usecols=["date", "hosp_covid"]).groupby("date", as_index=False).sum()

    url = "https://raw.githubusercontent.com/MoH-Malaysia/covid19-public/main/epidemic/icu.csv"
    icu = pd.read_csv(url, usecols=["date", "icu_covid"]).groupby("date", as_index=False).sum()

    df = (
        pd.merge(hosp, icu, on="date", how="outer", validate="one_to_one")
        .melt(id_vars="date", var_name="indicator")
        .assign(entity="Malaysia")
    )

    df.loc[:, "indicator"] = df.indicator.replace(
        {
            "hosp_covid": "Daily hospital occupancy",
            "icu_covid": "Daily ICU occupancy",
        }
    )

    return df
