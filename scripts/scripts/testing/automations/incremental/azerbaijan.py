import os
import pandas as pd

from cowidev.utils.utils import get_project_dir
from cowidev.utils import get_soup, clean_count
from cowidev.utils.clean.dates import localdate


def main():
    path = os.path.join(get_project_dir(), "scripts", "scripts", "testing", "automated_sheets", "Azerbaijan.csv")
    data = pd.read_csv(path).sort_values(by="Date", ascending=False)

    source_url = "https://koronavirusinfo.az/az/page/statistika/azerbaycanda-cari-veziyyet"

    soup = get_soup(source_url)

    element = soup.find_all("div", class_="gray_little_statistic")[5].find("strong")
    cumulative_total = clean_count(element.text)

    if cumulative_total > data["Cumulative total"].max():
        new = pd.DataFrame(
            {
                "Cumulative total": cumulative_total,
                "Date": [localdate("Asia/Baku")],
                "Country": "Azerbaijan",
                "Units": "tests performed",
                "Source URL": source_url,
                "Source label": "Cabinet of Ministers of Azerbaijan",
            }
        )

        df = pd.concat([new, data], sort=False)
        df.to_csv(path, index=False)


if __name__ == "__main__":
    main()
