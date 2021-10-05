import datetime
import pandas as pd
import numpy as np


def main():
    df = pd.read_csv(
        "https://sisa.msal.gov.ar/datos/descargas/covid-19/files/Covid19Determinaciones.zip",
        usecols=["fecha", "total", "positivos"],
    )

    # Occasional errors where some lab inserts data before 2020
    df = df[df.fecha >= "2020"]

    df = df.groupby("fecha", as_index=False).sum()

    df.columns = ["Date", "Daily change in cumulative total", "positive"]

    df["Positive rate"] = (
        df.positive.rolling(7).sum().div(df["Daily change in cumulative total"].rolling(7).sum()).round(3)
    )

    df = df.drop(columns=["positive"])
    df = df[df["Daily change in cumulative total"] > 0]

    df["Country"] = "Argentina"
    df["Units"] = "tests performed"
    df["Notes"] = np.nan
    df[
        "Source URL"
    ] = "https://datos.gob.ar/dataset/salud-covid-19-determinaciones-registradas-republica-argentina/archivo/salud_0de942d4-d106-4c74-b6b2-3654b0c53a3a"
    df["Source label"] = "Government of Argentina"

    df.to_csv("automated_sheets/Argentina - tests performed.csv", index=False)


if __name__ == "__main__":
    main()
