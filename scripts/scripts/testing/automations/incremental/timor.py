import os
import re

import pandas as pd

from cowidev.utils import get_soup
from cowidev.utils.clean.dates import localdatenow


def main():
    output_file = "automated_sheets/Timor.csv"
    url = "https://covid19.gov.tl/dashboard/"
    soup = get_soup(url, "html.parser")

    stats = soup.select("#testing .c-green .wdt-column-sum")[0].text
    count = int("".join(re.findall("[0-9]", stats)))
    # print(count)

    date_str = localdatenow("Asia/Dili")
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
