import datetime
import io
import re
import sys

import requests
import pandas as pd

DOMAIN = "https://www.mhlw.go.jp"

METADATA = {
    "source_url": DOMAIN + "/stf/seisakunitsuite/newpage_00023.html",
    "source_url_ref": DOMAIN + "/stf/seisakunitsuite/newpage_00023.html",
    "source_name": "Ministry of Health, Labour and Welfare",
    "entity": "Japan",
}

SEARCH_RE = re.compile(
        r'新型コロナウイルス感染症患者の療養状況等及び入院患者受入病床数等に関する調査結果（'
        r'(20.*?)年(.*?)月(.*?)日.*?pdf.*?href="(.*?)"')

def find_hospitalized(df: pd.DataFrame) -> pd.Series:
    for col in df:
        for obj in df[col][0:5]:
            if type(obj) is str and '入院者数' in obj:
                return df[col]
    return None

def main():
    response = requests.get(METADATA["source_url"]).content.decode('utf-8').replace('\n', '')
    result_df = pd.DataFrame()
    for result in reversed(SEARCH_RE.findall(response)):
        year, month, day, path = result
        if not path.endswith('.xlsx'):
            continue
        year, month, day = int(year), int(month), int(day)
        url = DOMAIN + path
        df = pd.read_excel(url)
        hospitalized_col = find_hospitalized(df)
        if hospitalized_col is None:
            continue
        hospitalized_col = hospitalized_col.dropna()
        hospitalized = hospitalized_col.iloc[-1]
        row = {
                'date': datetime.datetime(year=year, month=month, day=day),
                'indicator': 'Daily hospital occupancy',
                'value': hospitalized,
                'entity': METADATA['entity'],
              }
        result_df = result_df.append(row, ignore_index=True)
    
    return result_df, METADATA

if __name__ == '__main__':
    main()
