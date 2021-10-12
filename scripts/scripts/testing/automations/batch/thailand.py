import pandas as pd


def main():

    file_url = "https://data.go.th/dataset/9f6d900f-f648-451f-8df4-89c676fce1c4/resource/0092046c-db85-4608-b519-ce8af099315e/download"
    general_url = "https://data.go.th/dataset/covid-19-testing-data"

    df = pd.read_csv(file_url, usecols=["Date", "Total Testing"])

    df["Date"] = pd.to_datetime(df.Date, format="%d/%m/%Y", errors="coerce")

    df = df.dropna(subset=["Date"]).rename(columns={"Total Testing": "Daily change in cumulative total"})

    df.loc[:, "Country"] = "Thailand"
    df.loc[:, "Units"] = "tests performed"
    df.loc[:, "Source URL"] = general_url
    df.loc[:, "Source label"] = "Department of Medical Sciences Ministry of Public Health"
    df.loc[:, "Notes"] = pd.NA

    df = df[df["Daily change in cumulative total"] > 0]

    df.to_csv("automated_sheets/Thailand.csv", index=False)


if __name__ == "__main__":
    main()
