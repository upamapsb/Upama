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
    hosp_flow["date"] = pd.to_datetime(hosp_flow["date"])
    hosp_flow = hosp_flow[hosp_flow.date.dt.dayofweek == 6]

    # ICU admissions and patients
    icu = (
        pd.read_csv(ICU_URL, usecols=["Datum", "Aktuelle_COVID_Faelle_ITS", "faelle_covid_erstaufnahmen"])
        .rename(columns={"Datum": "date"})
        .groupby("date", as_index=False)
        .sum()
    )
    icu["date"] = pd.to_datetime(icu.date.str.slice(0, 10))

    icu_stock = icu[["date", "Aktuelle_COVID_Faelle_ITS"]].copy()

    icu_flow = icu[["date", "faelle_covid_erstaufnahmen"]].copy()
    icu_flow = icu_flow[icu_flow.faelle_covid_erstaufnahmen > 0]
    icu_flow["date"] = (icu_flow["date"] + pd.to_timedelta(6 - icu_flow["date"].dt.dayofweek, unit="d")).dt.date
    icu_flow = icu_flow[icu_flow["date"] <= datetime.date.today()]
    icu_flow = icu_flow.groupby("date", as_index=False).agg({"faelle_covid_erstaufnahmen": ["sum", "count"]})
    icu_flow.columns = ["date", "faelle_covid_erstaufnahmen", "count"]
    icu_flow = icu_flow[icu_flow["count"] == 7]
    icu_flow = icu_flow.drop(columns="count")
    icu_flow["date"] = pd.to_datetime(icu_flow["date"])

    # Merge
    df = (
        pd.merge(hosp_flow, icu_stock, on="date", how="outer")
        .merge(icu_flow, on="date", how="outer")
        .sort_values("date")
        .melt(
            id_vars="date",
            value_vars=["7T_Hospitalisierung_Faelle", "Aktuelle_COVID_Faelle_ITS", "faelle_covid_erstaufnahmen"],
            var_name="indicator",
        )
    )
    df["indicator"] = df["indicator"].replace(
        {
            "Aktuelle_COVID_Faelle_ITS": "Daily ICU occupancy",
            "7T_Hospitalisierung_Faelle": "Weekly new hospital admissions",
            "faelle_covid_erstaufnahmen": "Weekly new ICU admissions",
        },
    )

    df.loc[:, "entity"] = "Germany"
    df.loc[:, "iso_code"] = "DEU"
    df.loc[:, "population"] = 83900471

    return df


if __name__ == "__main__":
    main()
