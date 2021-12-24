import datetime

import pandas as pd

from cowidev.utils.web.scraping import get_soup

SOURCE_PAGE = "https://www.mscbs.gob.es/profesionales/saludPublica/ccayes/alertasActual/nCov/capacidadAsistencial.htm"


def main() -> pd.DataFrame:

    print("Downloading Spain data…")

    soup = get_soup(SOURCE_PAGE)
    url = soup.find(class_="informacion").find("a")["href"]
    url = "https://www.mscbs.gob.es/profesionales/saludPublica/ccayes/alertasActual/nCov/" + url

    df = pd.read_csv(
        url, usecols=["Fecha", "Unidad", "OCUPADAS_COVID19", "INGRESOS_COVID19"], encoding="Latin-1", sep=";"
    )

    df["Fecha"] = pd.to_datetime(df.Fecha, dayfirst=True).dt.date.astype(str)

    df.loc[df.Unidad.str.contains("U. Críticas"), "Unidad"] = "ICU"

    df = (
        df.groupby(["Fecha", "Unidad"], as_index=False)
        .sum()
        .sort_values("Unidad")
        .pivot(index="Fecha", columns="Unidad")
        .reset_index()
        .sort_values("Fecha")
    )
    df.columns = ["date", "hosp_stock", "icu_stock", "hosp_flow", "icu_flow"]

    df["hosp_flow"] = df.hosp_flow.rolling(7).sum()
    df["icu_flow"] = df.icu_flow.rolling(7).sum()

    df = df.melt("date", var_name="indicator").dropna(subset=["value"])
    df["indicator"] = df.indicator.replace(
        {
            "hosp_flow": "Weekly new hospital admissions",
            "icu_flow": "Weekly new ICU admissions",
            "hosp_stock": "Daily hospital occupancy",
            "icu_stock": "Daily ICU occupancy",
        }
    )

    df["entity"] = "Spain"

    return df


if __name__ == "__main__":
    main()
