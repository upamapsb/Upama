import re

import requests
import pandas as pd

from cowidev.utils.web.scraping import get_soup, request_text


def main():

    data = pd.read_csv("automated_sheets/Jamaica.csv")

    # get and parse daily updates page
    general_url = "https://www.moh.gov.jm/updates/coronavirus/covid-19-clinical-management-summary/"

    soup = get_soup(general_url)

    # find and assign url
    source_url = soup.find("div", class_="block-content").find("h2").find("a").attrs["href"]

    # find and assign date
    date = soup.find("div", class_="block-content").find("h2").text.upper()
    date = re.search(
        r"(JAN(?:UARY)?|FEB(?:RUARY)?|MAR(?:CH)?|APR(?:IL)?|MAY|JUN(?:E)?|JUL(?:Y)?|AUG(?:UST)?|SEP(?:TEMBER)?|OCT(?:OBER)?|NOV(?:EMBER)?|DEC(?:EMBER)?)\s+(\d{1,2})\,\s*(\d{4})",
        date,
    ).group(0)
    date = str(pd.to_datetime(date).date())

    # get and parse table; find and assign values
    text = request_text(source_url, mode="raw")
    table = pd.read_html(text, index_col=0)[0]

    cumulative_total = int(table.loc["TOTAL TESTS CUMULATIVE", table.shape[1]])

    if cumulative_total > data["Cumulative total"].max() and date > data["Date"].max():

        # create and append new row
        new = pd.DataFrame(
            {
                "Cumulative total": cumulative_total,
                "Date": [date],
                "Country": "Jamaica",
                "Units": "samples tested",
                "Source URL": source_url,
                "Source label": "Jamaica Ministry of Health and Wellness",
            }
        )

        df = pd.concat([new, data], sort=False)
        df.to_csv("automated_sheets/Jamaica.csv", index=False)


if __name__ == "__main__":
    main()
