import datetime

import pandas as pd

SOURCE_URL = "https://cnecovid.isciii.es/covid19/resources/casos_hosp_uci_def_sexo_edad_provres.csv"


def main() -> pd.DataFrame:

    print("Downloading Spain data…")
    df = pd.read_csv(SOURCE_URL, usecols=["fecha", "num_hosp", "num_uci"])

    df = df.rename(columns={"fecha": "date"}).groupby("date", as_index=False).sum().sort_values("date")
    df = df[pd.to_datetime(df.date).dt.date < (datetime.date.today() - datetime.timedelta(days=3))]

    df["num_hosp"] = df.num_hosp.rolling(7).sum()
    df["num_uci"] = df.num_uci.rolling(7).sum()

    df = df.melt("date", var_name="indicator").dropna(subset=["value"])
    df["indicator"] = df.indicator.replace(
        {
            "num_hosp": "Weekly new hospital admissions",
            "num_uci": "Weekly new ICU admissions",
        }
    )

    df["entity"] = "Spain"

    return df


if __name__ == "__main__":
    main()
