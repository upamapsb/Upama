import re

import pandas as pd
import requests
from bs4 import BeautifulSoup


def main():

    data = pd.read_csv("automated_sheets/Greece.csv")

    url = "https://covid19.gov.gr/"
    req = requests.get(url)
    soup = BeautifulSoup(req.content, "html.parser")

    count = int(
        soup.select(".elementor-element-9df72a6 .elementor-size-default")[0]
        .text.replace("ΣΥΝΟΛΟ: ", "")
        .replace(".", "")
    )

    date_str = re.search(r"Τελευταία ενημέρωση\: ([\d/]{,10})", soup.text).group(1)
    date_str = str(pd.to_datetime(date_str, dayfirst=True).date())

    if count > data["Cumulative total"].max() and date_str > data["Date"].max():

        new = pd.DataFrame(
            {
                "Country": "Greece",
                "Date": [date_str],
                "Cumulative total": count,
                "Source URL": url,
                "Source label": "National Organization of Public Health",
                "Units": "samples tested",
            }
        )

        pd.concat([new, data], sort=False).sort_values("Date").to_csv("automated_sheets/Greece.csv", index=False)


if __name__ == "__main__":
    main()
