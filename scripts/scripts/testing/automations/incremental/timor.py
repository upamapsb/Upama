import os
from datetime import date

import requests
import pandas as pd
from bs4 import BeautifulSoup


def main():
    output_file = "automated_sheets/Timor.csv"
    url = "https://covid19.gov.tl/dashboard/"
    req = requests.get(url)
    soup = BeautifulSoup(req.text, "html.parser")

    stats = soup.find_all("span", attrs={"class": "wdt-column-sum-value"})
    count = int(stats[5].text.replace(",", ""))
    # print(count)

    date_str = date.today().strftime("%Y-%m-%d")
    df = pd.DataFrame(
        {
            "Country": "Timor",
            "Date": [date_str],
            "Cumulative total": count,
            "Source URL": url,
            "Source label": "Ministry of Health",
            "Units": "tests performed",
            "Notes": pd.NA,
        }
    )

    if os.path.isfile(output_file):
        existing = pd.read_csv(output_file)
        if count > existing["Cumulative total"].max() and date_str > existing["Date"].max():
            df = pd.concat([df, existing]).sort_values("Date", ascending=False).drop_duplicates()
            df.to_csv(output_file, index=False)


if __name__ == "__main__":
    main()
