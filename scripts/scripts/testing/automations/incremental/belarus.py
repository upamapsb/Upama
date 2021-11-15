import re
import os

import pandas as pd

from cowidev.utils.utils import get_project_dir
from cowidev.utils.web import get_soup
from cowidev.utils.clean import clean_count, clean_date


def main():

    path = os.path.join(get_project_dir(), "scripts", "scripts", "testing", "automated_sheets", "Belarus.csv")
    data = pd.read_csv(path)

    # get and parse daily updates page
    general_url = "https://www.belarus.by/en/press-center/press-release/?page=1"

    # Get an ssl.SSLCertVerificationError error without verify=False
    soup = get_soup(general_url, verify=False)

    news_tags = soup.select(".news_text a[href]")
    for news_tag in news_tags:

        url = f"https://www.belarus.by{news_tag.get('href')}"
        soup_page = get_soup(url, verify=False)
        text_page = soup_page.find(class_="ic").text
        match_text = re.search(r"Belarus (has )?performed [\d,]+ tests", text_page)

        # Go to next URL if unable to match the pattern
        if match_text is not None:

            cumulative_total = clean_count(re.search(r"[\d,]+", match_text.group(0)).group(0))

            date_raw = soup_page.find(class_="pages_header_inner").text
            date = clean_date(date_raw, "%d %b %Y")

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
                        "Cumulative total": cumulative_total,
                    }
                )

                df = pd.concat([data, new], sort=False)
                df.to_csv(path, index=False)
            break


if __name__ == "__main__":
    main()
