import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import date


def main():
    data = pd.read_csv("automated_sheets/Nigeria.csv").sort_values(by="Date", ascending=False)

    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36"
    }
    source_url = "http://covid19.ncdc.gov.ng/"

    req = requests.get(source_url, headers=headers)
    soup = BeautifulSoup(req.content, "html.parser")

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
        df.to_csv("automated_sheets/Nigeria.csv", index=False)


if __name__ == "__main__":
    main()
