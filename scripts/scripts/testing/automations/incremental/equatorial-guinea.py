import os
from datetime import date

import pandas as pd

from cowidev.utils import get_soup, clean_count


def main():
    url = "https://guineasalud.org/estadisticas/"
    location = "Equatorial Guinea"
    output_file = f"automated_sheets/{location}.csv"

    soup = get_soup(url)
    stats = soup.find_all("tr")
    count = clean_count(stats[9].find_all("td")[-1].text)

    date_str = date.today().strftime("%Y-%m-%d")
    df = pd.DataFrame(
        {
            "Country": location,
            "Date": [date_str],
            "Cumulative total": count,
            "Source URL": url,
            "Source label": "Ministerio de Sanidad y Bienestar Social",
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
