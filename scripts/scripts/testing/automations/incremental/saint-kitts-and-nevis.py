import os

import pandas as pd

from cowidev.utils import get_soup
from cowidev.utils.clean.dates import localdatenow


def main():
    url = "https://covid19.gov.kn/src/stats2/"
    location = "Saint Kitts and Nevis"
    output_file = f"automated_sheets/{location}.csv"

    soup = get_soup(url)
    df = pd.read_html(str(soup.find("table")))[0]
    count = df.loc[df[0] == "No. of Persons Tested", 1].item()
    # print(count)

    date_str = localdatenow("America/St_Kitts")
    df = pd.DataFrame(
        {
            "Country": location,
            "Date": [date_str],
            "Cumulative total": count,
            "Source URL": url,
            "Source label": "Ministry of Health",
            "Units": "people tested",
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
