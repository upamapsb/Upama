import pandas as pd


def main() -> pd.DataFrame:

    print("Downloading South Africa data…")
    url = "https://docs.google.com/spreadsheets/d/e/2PACX-1vRGCkIwakQ5rpfXky9FZhDwr3qUgerfhBLSzn9OsA79yQ_2G_y-_Ns9JjRJZWXD5kxJ3qicoL7bHGjE/pub?gid=1044172863&single=true&output=csv"
    df = pd.read_csv(url, usecols=["Week ending:", "National"])

    df = df.assign(indicator="Weekly new hospital admissions").rename(
        columns={
            "Week ending:": "date",
            "National": "value",
        }
    )

    df["date"] = pd.to_datetime(df.date, dayfirst=True).dt.date.astype(str)
    df["entity"] = "South Africa"

    return df
