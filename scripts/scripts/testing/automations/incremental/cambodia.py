import os

import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import date

def main():

    data = pd.read_csv("automated_sheets/Cambodia.csv")

    url = 'http://cdcmoh.gov.kh/'
    req = requests.get(url)
    soup = BeautifulSoup(req.text, "html.parser")

    count = int(soup.select("span:nth-child(1) strong span")[0].text.replace(" ", "").replace(",", ""))

    date_str = date.today().strftime("%Y-%m-%d")

    if count > data["Cumulative total"].max() and date_str > data["Date"].max():

        new = pd.DataFrame({
            'Country': 'Cambodia',
            'Date': [date_str],
            'Cumulative total': count,
            'Source URL': url,
            'Source label': 'CDCMOH',
            'Units': 'tests performed',
        })

        data = pd.concat([new, data], sort=False)

    data.to_csv("automated_sheets/Cambodia.csv", index=False)

if __name__ == '__main__':
    main()
