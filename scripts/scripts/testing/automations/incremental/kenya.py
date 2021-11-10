import os
from cowidev.utils.utils import get_project_dir
from cowidev.utils.web import get_soup

import pandas as pd
import datetime


def main():
    path = os.path.join(get_project_dir(), "scripts", "scripts", "testing", "automated_sheets", "Kenya.csv")
    data = pd.read_csv(path).sort_values(by="Date", ascending=False)

    source_url = "http://covidkenya.org/"

    soup = get_soup(source_url)

    cumulative_total = int(
        soup.find("div", class_="elementor-element-b36fad5").find(class_="elementor-text-editor").text.replace(",", "")
    )

    year = datetime.datetime.now().year
    Date = soup.select(".elementor-element-75168b2 p")[0].text.replace("[Updated on ", "").replace(" [18:04]", "")
    Date = str(year) + "-" + pd.to_datetime(Date, format="%B %d").strftime("%m-%d")

    if cumulative_total > data["Cumulative total"].max():
        new = pd.DataFrame(
            {
                "Cumulative total": cumulative_total,
                "Date": [Date],
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
