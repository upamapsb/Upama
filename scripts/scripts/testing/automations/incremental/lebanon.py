import os

import pandas as pd

from cowidev.utils import clean_count, get_soup
from cowidev.utils.utils import get_project_dir
from cowidev.utils.clean import extract_clean_date


def main():
    path = os.path.join(get_project_dir(), "scripts", "scripts", "testing", "automated_sheets", "Lebanon.csv")
    data = pd.read_csv(path).sort_values(by="Date", ascending=False)

    source_url = "https://corona.ministryinfo.gov.lb/"

    soup = get_soup(source_url)

    element = soup.find("h1", class_="s-counter3")
    cumulative_total = clean_count(element.text)

    date_raw = soup.select(".last-update strong")[0].text
    date = extract_clean_date(date_raw, regex=r"([A-Za-z]+ \d+)", date_format="%b %d", replace_year=2021)

    if cumulative_total > data["Cumulative total"].max():
        new = pd.DataFrame(
            {
                "Cumulative total": cumulative_total,
                "Date": [date],
                "Country": "Lebanon",
                "Units": "tests performed",
                "Source URL": source_url,
                "Source label": "Lebanon Ministry of Health",
            }
        )

        df = pd.concat([new, data], sort=False)
        df.to_csv(path, index=False)


if __name__ == "__main__":
    main()
