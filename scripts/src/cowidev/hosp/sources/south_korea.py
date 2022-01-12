import pandas as pd


METADATA = {
    "source_url": "https://github.com/WWolf/korea-covid19-hosp-data/raw/main/hospitalization.csv",
    "source_url_ref": "http://ncov.mohw.go.kr/en/bdBoardList.do?brdId=16&brdGubun=161&dataGubun=&ncvContSeq=&contSeq=&board_id=",
    "source_name": "Ministry of Health and Welfare, via WWolf on GitHub",
    "entity": "South Korea",
}


def main():
    df = (
        pd.read_csv(
            METADATA["source_url"],
            usecols=[
                "Date",
                "Hospitalizations with moderate to severe symptoms",
                "New hospital admissions (daily)",
            ],
            na_values="NA",
        )
        .rename(columns={"Date": "date"})
        .sort_values("date")
    )

    df["New hospital admissions (daily)"] = df["New hospital admissions (daily)"].rolling(7).sum()

    df = df.melt("date", var_name="indicator").dropna(subset=["value"])
    df["indicator"] = df.indicator.replace(
        {
            "Hospitalizations with moderate to severe symptoms": "Daily ICU occupancy",
            "New hospital admissions (daily)": "Weekly new hospital admissions",
        }
    )

    df["entity"] = METADATA["entity"]

    return df, METADATA


if __name__ == "__main__":
    main()
