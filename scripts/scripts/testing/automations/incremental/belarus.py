import re
import requests
import datetime
import pandas as pd
from bs4 import BeautifulSoup


def main():

    data = pd.read_csv("automated_sheets/Belarus.csv")

    # get and parse daily updates page
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36"
    }
    general_url = "https://www.belarus.by/en/press-center/press-release/?page=1"

    # Get an ssl.SSLCertVerificationError error without verify=False
    req = requests.get(general_url, headers=headers, verify=False)
    soup = BeautifulSoup(req.content, "html.parser")

    news_tags = soup.select(".news_text a[href]")
    for news_tag in news_tags:
        url = "https://www.belarus.by" + news_tag.get('href')
        req_page = requests.get(url, headers=headers, verify=False)
        soup_page = BeautifulSoup(req_page.content, "html.parser")
        text_page = soup_page.find(class_="ic").get_text()
        match_text = re.search(r"Belarus (has )?performed [\d,]+ tests", text_page)
        # Go to next URL if unable to match the pattern
        if match_text is not None:

            cumulative_total = int(re.search(r"[\d,]+", match_text.group(0)).group(0).replace(",", ""))

            # Obtain the date for this press release
            date_page = soup_page.find(class_="pages_header_inner").get_text()
            date = str(pd.to_datetime(date_page).date())

            if cumulative_total > data["Cumulative total"].max() and date > data["Date"].max():
                # create and append new row
                new = pd.DataFrame(
                    {
                        "Date": [date],
                        "Country": "Belarus",
                        "Units": "tests performed",
                        "Source URL": url,
                        "Source label": "Government of Belarus",
                        "Notes": pd.NA,
                        "Cumulative total": cumulative_total
                    }
                )

                df = pd.concat([data, new], sort=False)
                df.to_csv("automated_sheets/Belarus.csv", index=False)
            break


if __name__ == "__main__":
    main()
