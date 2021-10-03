import re
import time

import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options


SOURCE_URL = "https://www.koronavirus.hr/najnovije/ukupno-dosad-382-zarazene-osobe-u-hrvatskoj/35"


def get_tests_snapshot():
    op = Options()
    op.add_argument("--headless")

    with webdriver.Chrome(options=op) as driver:
        driver.get(SOURCE_URL)
        time.sleep(2)
        all_text = driver.find_element_by_tag_name("body").text

    tests_cumul = int(re.search(r"Do danas je ukupno testirano ([\d\.]+) osoba", all_text).group(1).replace(".", ""))
    date = str(pd.to_datetime(re.search(r"Objavljeno: ([\d\.]{10})", all_text).group(1), dayfirst=True).date())

    return date, tests_cumul


def main():
    date, tests_cumul = get_tests_snapshot()

    existing = pd.read_csv("automated_sheets/Croatia.csv")

    if tests_cumul > existing["Cumulative total"].max() and date > existing["Date"].max():

        new = pd.DataFrame(
            {
                "Country": "Croatia",
                "Date": [date],
                "Cumulative total": tests_cumul,
                "Source URL": SOURCE_URL,
                "Source label": "Government of Croatia",
                "Units": "people tested",
                "Notes": pd.NA,
            }
        )

        df = pd.concat([new, existing]).sort_values("Date")
        df.to_csv("automated_sheets/Croatia.csv", index=False)


if __name__ == "__main__":
    main()
