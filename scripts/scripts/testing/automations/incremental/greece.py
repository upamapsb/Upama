import os

import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import date

def main():

    data = pd.read_csv("automated_sheets/Greece.csv")

    url = 'https://covid19.gov.gr/'
    req = requests.get(url)
    soup = BeautifulSoup(req.content, "html.parser")

    count = int(soup.select(".elementor-element-9df72a6 .elementor-size-default")[0].text.replace("ΣΥΝΟΛΟ: ","").replace(".", ""))

    date_str = date.today().strftime("%Y-%m-%d")

    if count > data["Cumulative total"].max() and date_str > data["Date"].max():

        new = pd.DataFrame({
            'Country': 'Greece',
            'Date': [date_str],
            'Cumulative total': count,
            'Source URL': url,
            'Source label': 'National Organization of Public Health',
            'Units': 'samples tested',
        })

        data = pd.concat([new, data], sort=False)

    data.to_csv("automated_sheets/Greece.csv", index=False)

if __name__ == '__main__':
    main()
