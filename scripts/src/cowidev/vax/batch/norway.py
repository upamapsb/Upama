import os
import time

import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

from cowidev.utils.clean import clean_date_series


class Norway:
    def __init__(self) -> None:
        self.location = "Norway"
        self.source_url = "https://www.fhi.no/sv/vaksine/koronavaksinasjonsprogrammet/koronavaksinasjonsstatistikk/"

    def read(self):
        # Options for Chrome WebDriver
        op = Options()
        op.add_argument("--disable-notifications")
        op.add_argument("--headless")
        op.add_experimental_option(
            "prefs",
            {
                "download.prompt_for_download": False,
                "download.directory_upgrade": True,
                "safebrowsing.enabled": True,
            },
        )

        with webdriver.Chrome(options=op) as driver:
            driver.implicitly_wait(15)
            # Setting Chrome to trust downloads
            driver.command_executor._commands["send_command"] = (
                "POST",
                "/session/$sessionId/chromium/send_command",
            )
            params = {
                "cmd": "Page.setDownloadBehavior",
                "params": {"behavior": "allow", "downloadPath": "."},
            }
            _ = driver.execute("send_command", params)

            driver.get(self.source_url)
            element = driver.find_element_by_class_name("highcharts-exporting-group")
            time.sleep(2)
            self._scroll_till_element_middle(driver, element)
            element.click()
            time.sleep(2)
            for item in driver.find_elements_by_class_name("highcharts-menu-item"):
                if item.text == "Last ned CSV":
                    self._scroll_till_element_middle(driver, item)
                    item.click()
                    time.sleep(2)
                    break

        df = read_csv_multiple_separators(
            "./antall-personer-vaksiner.csv",
            separators=[";", ","],
            usecols=[
                "Kategori",
                "Kumulativt antall personer vaksinert med 1.dose",
                "Kumulativt antall personer vaksinert med 2.dose",
            ],
        )
        if not len(df) > 10:
            raise ValueError("Check source data, to few entries to be a timeseries")
        os.remove("./antall-personer-vaksiner.csv")
        return df

    def _scroll_till_element_middle(self, driver, element):
        desired_y = (element.size["height"] / 2) + element.location["y"]
        current_y = (driver.execute_script("return window.innerHeight") / 2) + driver.execute_script(
            "return window.pageYOffset"
        )
        scroll_y_by = desired_y - current_y
        driver.execute_script("window.scrollBy(0, arguments[0]);", scroll_y_by)

    def pipe_rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.rename(
            columns={
                "Kumulativt antall personer vaksinert med 1.dose": "people_vaccinated",
                "Kumulativt antall personer vaksinert med 2.dose": "people_fully_vaccinated",
                "Kategori": "date",
            }
        )

    def pipe_date(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(date=clean_date_series(df.date))

    def pipe_vaccine(self, df: pd.DataFrame) -> pd.DataFrame:
        def _enrich_vaccine(date: str):
            if date < "2021-01-15":
                return "Pfizer/BioNTech"
            elif "2021-01-15" <= date < "2021-02-10":
                return "Moderna, Pfizer/BioNTech"
            elif "2021-02-10" <= date < "2021-03-11":
                return "Moderna, Oxford/AstraZeneca, Pfizer/BioNTech"
            elif "2021-03-11" <= date:
                return "Moderna, Pfizer/BioNTech"

        return df.assign(vaccine=df.date.apply(_enrich_vaccine))

    def pipe_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(total_vaccinations=df.people_vaccinated + df.people_fully_vaccinated.fillna(0))

    def pipe_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(
            source_url=self.source_url,
            location=self.location,
        )

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.pipe(self.pipe_rename_columns)
            .pipe(self.pipe_date)
            .pipe(self.pipe_vaccine)
            .pipe(self.pipe_metrics)
            .pipe(self.pipe_metadata)
        )

    def export(self, paths):
        df = self.read().pipe(self.pipeline)
        df.to_csv(paths.tmp_vax_out(self.location), index=False)


def main(paths):
    Norway().export(paths)


def read_csv_multiple_separators(filepath: str, separators: list, usecols: list) -> pd.DataFrame:
    """Read a csv using potential separator candidates.

    Args:
        filepath (str): Path to file.
        separators (list): List of potential separator candidates. The file is read with the different candidate
                        separators. The one that is most likely to be the actual separator is used. Note that the list
                        is checked in sequentially.
        usecols (list): Columns to load.

    Returns:
        pandas.DataFrame: Loaded csv
    """
    for sep in separators:
        df = pd.read_csv(filepath, sep=sep)
        if df.shape[1] != 1:
            return df[usecols]
    raise Exception("Check regional settings and the delimiter of the downloaded CSV file.")
