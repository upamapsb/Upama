import pandas as pd

MAIN_SOURCE = (
    "https://github.com/pcm-dpc/COVID-19/raw/master/dati-andamento-nazionale/dpc-covid19-ita-andamento-nazionale.csv"
)
HOSP_FLOW_SOURCE = "https://covid19.infn.it/iss/csv_part/iss_bydate_italia_ricoveri.csv"


def main() -> pd.DataFrame:

    print("Downloading Italy data…")

    hosp_flow = pd.read_csv(HOSP_FLOW_SOURCE, usecols=["data", "casi_media7gg"]).rename(columns={"data": "date"})
    hosp_flow["casi_media7gg"] = hosp_flow.casi_media7gg.mul(7)

    df = (
        pd.read_csv(
            MAIN_SOURCE, usecols=["data", "totale_ospedalizzati", "terapia_intensiva", "ingressi_terapia_intensiva"]
        )
        .rename(columns={"data": "date"})
        .sort_values("date")
    )
    df["date"] = df.date.astype(str).str.slice(0, 10)
    df["ingressi_terapia_intensiva"] = df.ingressi_terapia_intensiva.rolling(7).sum()

    df = (
        pd.merge(hosp_flow, df, on="date", how="outer", validate="one_to_one")
        .melt("date", var_name="indicator")
        .dropna(subset=["value"])
        .assign(entity="Italy")
    )
    df["indicator"] = df.indicator.replace(
        {
            "casi_media7gg": "Weekly new hospital admissions",
            "ingressi_terapia_intensiva": "Weekly new ICU admissions",
            "totale_ospedalizzati": "Daily hospital occupancy",
            "terapia_intensiva": "Daily ICU occupancy",
        }
    )

    return df


if __name__ == "__main__":
    main()
