import os

import pandas as pd

from cowidev.utils.utils import get_project_dir
from cowidev.utils.web import get_soup
from cowidev.utils.clean import clean_count
from cowidev.utils.clean import extract_clean_date


def main():
    path = os.path.join(get_project_dir(), "scripts", "scripts", "testing", "automated_sheets", "Kenya.csv")
    data = pd.read_csv(path).sort_values(by="Date", ascending=False)

    source_url = "http://covidkenya.org/"

    soup = get_soup(source_url)

    element = soup.find("div", class_="elementor-element-b36fad5").find(class_="elementor-text-editor")
    cumulative_total = clean_count(element.text)

    date_raw = soup.select(".elementor-element-75168b2 p")[0].text
    date = extract_clean_date(
        date_raw, regex=r"\[Updated on ([A-Za-z]+ \d+) \[\d\d:\d\d\]", date_format="%B %d", replace_year=2021
    )

    if cumulative_total > data["Cumulative total"].max():
        new = pd.DataFrame(
            {
                "Cumulative total": cumulative_total,
                "Date": [date],
                "Country": "Kenya",
                "Units": "samples tested",
                "Source URL": source_url,
                "Source label": "Kenya Ministry of Health",
            }
        )

        df = pd.concat([new, data], sort=False)
        df.to_csv(path, index=False)


if __name__ == "__main__":
    main()
