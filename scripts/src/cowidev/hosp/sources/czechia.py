import pandas as pd

STOCK_URL = "https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19/hospitalizace.csv"
FLOW_URL = "https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19/nakazeni-hospitalizace-testy.csv"


def main() -> pd.DataFrame:

    print("Downloading Czechia data…")

    stock = pd.read_csv(STOCK_URL, usecols=["datum", "pocet_hosp", "jip"])
    stock = stock.rename(columns={"datum": "date"}).sort_values("date")

    flow = pd.read_csv(FLOW_URL, usecols=["datum", "nove_hospitalizace", "nove_jip"])
    flow = flow.rename(columns={"datum": "date"}).groupby("date", as_index=False).sum().sort_values("date")
    flow["nove_hospitalizace"] = flow.nove_hospitalizace.rolling(7).sum()
    flow["nove_jip"] = flow.nove_jip.rolling(7).sum()

    df = (
        pd.merge(flow, stock, on="date", how="outer", validate="one_to_one")
        .melt("date", var_name="indicator")
        .dropna(subset=["value"])
    )
    df["indicator"] = df.indicator.replace(
        {
            "pocet_hosp": "Daily hospital occupancy",
            "jip": "Daily ICU occupancy",
            "nove_hospitalizace": "Weekly new hospital admissions",
            "nove_jip": "Weekly new ICU admissions",
        }
    )

    df["entity"] = "Czechia"
    df["iso_code"] = "CZE"
    df["population"] = 10724553

    return df


if __name__ == "__main__":
    main()
