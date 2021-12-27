import pandas as pd

METADATA = {
    "source_url": {
        "hosp": "https://raw.githubusercontent.com/MoH-Malaysia/covid19-public/main/epidemic/hospital.csv",
        "icu": "https://raw.githubusercontent.com/MoH-Malaysia/covid19-public/main/epidemic/icu.csv",
    },
    "source_url_ref": "https://github.com/MoH-Malaysia/covid19-public",
    "source_name": "Ministry of Health",
    "entity": "Malaysia",
}


def main():
    hosp = (
        pd.read_csv(METADATA["source_url"]["hosp"], usecols=["date", "hosp_covid"])
        .groupby("date", as_index=False)
        .sum()
    )

    icu = (
        pd.read_csv(METADATA["source_url"]["icu"], usecols=["date", "icu_covid"]).groupby("date", as_index=False).sum()
    )

    df = (
        pd.merge(hosp, icu, on="date", how="outer", validate="one_to_one")
        .melt(id_vars="date", var_name="indicator")
        .assign(entity=METADATA["entity"])
    )

    df.loc[:, "indicator"] = df.indicator.replace(
        {
            "hosp_covid": "Daily hospital occupancy",
            "icu_covid": "Daily ICU occupancy",
        }
    )

    return df, METADATA
