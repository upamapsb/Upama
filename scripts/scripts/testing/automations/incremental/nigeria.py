import os
from cowidev.utils.utils import get_project_dir
from cowidev.utils.web import get_soup
import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import date


def main():
    path = os.path.join(get_project_dir(), "scripts", "scripts", "testing", "automated_sheets", "Nigeria.csv")
    data = pd.read_csv(path).sort_values(by="Date", ascending=False)

    source_url = "http://covid19.ncdc.gov.ng/"

    soup = get_soup(source_url)

    cumulative_total = int(soup.find("div", class_="col-xl-3").find("span").text.replace(",", ""))

    if cumulative_total > data["Cumulative total"].max():

        new = pd.DataFrame(
            {
                "Date": [date.today().strftime("%Y-%m-%d")],
                "Cumulative total": cumulative_total,
                "Country": "Nigeria",
                "Units": "samples tested",
                "Source URL": source_url,
                "Source label": "Nigeria Centre for Disease Control",
            }
        )

        df = pd.concat([new, data], sort=False)
        df.to_csv(path, index=False)


if __name__ == "__main__":
    main()
