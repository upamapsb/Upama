import pandas as pd

from cowidev.utils import get_soup, clean_count
from cowidev.utils.clean.dates import localdatenow


def main():

    data = pd.read_csv("automated_sheets/Cambodia.csv")

    url = "http://cdcmoh.gov.kh/"
    soup = get_soup(url)
    print(soup.select("span:nth-child(1) strong span"))

    count = clean_count(soup.select("span:nth-child(1) strong span")[0].text)

    date_str = localdatenow("Asia/Phnom_Penh")

    if count > data["Cumulative total"].max() and date_str > data["Date"].max():

        new = pd.DataFrame(
            {
                "Country": "Cambodia",
                "Date": [date_str],
                "Cumulative total": count,
                "Source URL": url,
                "Source label": "CDCMOH",
                "Units": "tests performed",
            }
        )

        data = pd.concat([new, data], sort=False)

    data.to_csv("automated_sheets/Cambodia.csv", index=False)


if __name__ == "__main__":
    main()
