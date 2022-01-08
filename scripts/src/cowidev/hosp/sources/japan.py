import datetime
import re
import requests

import pandas as pd

METADATA = {
    "source_url": "https://www.mhlw.go.jp/stf/seisakunitsuite/newpage_00023.html",
    "source_url_ref": "https://www.mhlw.go.jp/stf/seisakunitsuite/newpage_00023.html",
    "source_name": "Ministry of Health, Labour and Welfare",
    "entity": "Japan",
}

SEARCH_RE = re.compile(r"新型コロナウイルス感染症患者の療養状況等及び入院患者受入病床数等に関する調査結果（(20.*?)年(.*?)月(.*?)日.*?pdf.*?href=\"(.*?)\"")


def process_file(url: str, date: str) -> dict:
    print(url)
    df = pd.read_excel(url)

    for col in df:
        for obj in df[col][0:5]:
            if type(obj) is str and "入院者数" in obj:
                hospitalized_col = df[col]

    if hospitalized_col is None:
        return None

    hospitalized_col = hospitalized_col.dropna()
    hospitalized = hospitalized_col.iloc[-1]
    return {
        "date": date,
        "indicator": "Daily hospital occupancy",
        "value": hospitalized,
        "entity": METADATA["entity"],
    }


def main():
    response = requests.get(METADATA["source_url"]).content.decode("utf-8").replace("\n", "")

    records = []
    for result in reversed(SEARCH_RE.findall(response)):
        year, month, day, path = result
        if not path.endswith(".xlsx"):
            continue
        date = str(datetime.date(year=int(year), month=int(month), day=int(day)))
        records.append(process_file("https://www.mhlw.go.jp" + path, date))

    df = pd.DataFrame.from_records(records)
    return df, METADATA


if __name__ == "__main__":
    main()
