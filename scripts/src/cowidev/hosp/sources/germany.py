import datetime

import pandas as pd


ICU_URL = "https://diviexchange.blob.core.windows.net/%24web/zeitreihe-bundeslaender.csv"
HOSP_URL = "https://raw.githubusercontent.com/robert-koch-institut/COVID-19-Hospitalisierungen_in_Deutschland/master/Aktuell_Deutschland_COVID-19-Hospitalisierungen.csv"


def main() -> pd.DataFrame:

    print("Downloading Germany data…")

    # Hospital admissions
    hosp_flow = pd.read_csv(HOSP_URL, usecols=["Datum", "Bundesland", "Altersgruppe", "7T_Hospitalisierung_Faelle"])
    hosp_flow = (
        hosp_flow[(hosp_flow.Bundesland == "Bundesgebiet") & (hosp_flow.Altersgruppe == "00+")]
        .drop(columns=["Bundesland", "Altersgruppe"])
        .rename(columns={"Datum": "date"})
        .groupby("date", as_index=False)
        .sum()
    )

    # ICU admissions and patients
    icu = (
        pd.read_csv(ICU_URL, usecols=["Datum", "Aktuelle_COVID_Faelle_ITS", "faelle_covid_erstaufnahmen"])
        .rename(columns={"Datum": "date"})
        .groupby("date", as_index=False)
        .sum()
        .sort_values("date")
    )
    icu["date"] = icu.date.str.slice(0, 10)
    icu["faelle_covid_erstaufnahmen"] = icu.faelle_covid_erstaufnahmen.rolling(7).sum()

    # Merge
    df = (
        pd.merge(hosp_flow, icu, on="date", how="outer")
        .sort_values("date")
        .melt(id_vars="date", var_name="indicator")
        .dropna(subset=["value"])
    )
    df["indicator"] = df["indicator"].replace(
        {
            "Aktuelle_COVID_Faelle_ITS": "Daily ICU occupancy",
            "7T_Hospitalisierung_Faelle": "Weekly new hospital admissions",
            "faelle_covid_erstaufnahmen": "Weekly new ICU admissions",
        },
    )

    df["entity"] = "Germany"
    df["iso_code"] = "DEU"
    df["population"] = 83900471

    return df


if __name__ == "__main__":
    main()
