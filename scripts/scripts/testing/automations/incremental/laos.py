import datetime
import time

import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options


SOURCE_URL = "https://app.powerbi.com/view?r=eyJrIjoiM2JkZWRhNTQtNDY5YS00YWM3LWI4ZjUtNmExM2VmZDM4YjU5IiwidCI6ImNkOWNiOGVjLWU2MjEtNDcyYS05NzlhLTU0OWFiNWJhMjQ3MCIsImMiOjF9"


def get_tests_snapshot():
    op = Options()
    op.add_argument("--headless")

    with webdriver.Chrome(options=op) as driver:
        driver.get(SOURCE_URL)
        time.sleep(2)
        tests_cumul = driver.find_element_by_class_name("value").text

    assert "K" in tests_cumul
    tests_cumul = float(tests_cumul.replace(",", ".").replace("K", "")) * 1000

    date = str(datetime.date.today() - datetime.timedelta(days=1))
    return date, tests_cumul


def main():
    date, tests_cumul = get_tests_snapshot()

    existing = pd.read_csv("automated_sheets/Laos.csv")

    if tests_cumul > existing["Cumulative total"].max() and date > existing["Date"].max():

        new = pd.DataFrame(
            {
                "Country": "Laos",
                "Date": [date],
                "Cumulative total": tests_cumul,
                "Source URL": SOURCE_URL,
                "Source label": "Government of Laos",
                "Units": "people tested",
                "Notes": pd.NA,
            }
        )

        df = pd.concat([new, existing]).sort_values("Date")
        df.to_csv("automated_sheets/Laos.csv", index=False)


if __name__ == "__main__":
    main()
