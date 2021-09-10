import time

import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from cowidev.utils.clean import clean_count
from cowidev.utils.clean.dates import localdate
from cowidev.utils.web.scraping import get_driver, set_download_settings
from cowidev.vax.utils.incremental import enrich_data, increment
from cowidev.vax.utils.utils import get_latest_file


class Brazil:
    def __init__(self) -> None:
        self.location = "Brazil"
        self.source_url = "https://qsprod.saude.gov.br/extensions/DEMAS_C19Vacina/DEMAS_C19Vacina.html"
        self._download_path = "/tmp"

    def read(self) -> pd.Series:
        return self._parse_data()

    def _parse_data(self) -> pd.Series:
        with get_driver() as driver:
            set_download_settings(driver, self._download_path)
            driver.get(self.source_url)
            data_blocks = WebDriverWait(driver, 20).until(
                EC.visibility_of_all_elements_located((By.CLASS_NAME, "sn-kpi-data"))
            )
            for block in data_blocks:
                block_title = block.find_element_by_class_name("sn-kpi-measure-title").get_attribute("title")
                if "Total de Doses Aplicadas (Dose1)" in block_title:
                    people_vaccinated = block.find_element_by_class_name("sn-kpi-value").text
                elif "Total de Doses Aplicadas (Doses 2 e Única)" in block_title:
                    people_fully_vaccinated = block.find_element_by_class_name("sn-kpi-value").text
                elif "Total de Doses Aplicadas" in block_title:
                    total_vaccinations = block.find_element_by_class_name("sn-kpi-value").text
            doses_unique = self._parse_unique_doses(driver)

        ds = pd.Series(
            {
                "total_vaccinations": total_vaccinations,
                "people_vaccinated": people_vaccinated,
                "people_fully_vaccinated": people_fully_vaccinated,
            }
        ).transform(clean_count)
        ds.people_vaccinated = ds.people_vaccinated + doses_unique
        return ds

    def _parse_unique_doses(self, driver):
        time.sleep(10)
        elem = driver.find_element_by_id("QV1-export")
        elem.click()
        elem = elem.find_element_by_class_name("fa-download")
        elem.click()
        time.sleep(5)
        path = get_latest_file(self._download_path, "xlsx")
        df = pd.read_excel(path)
        doses_unique = df.loc[df.Fabricante == "JANSSEN", "Doses Aplicadas"].sum().item()
        return doses_unique

    def pipe_date(self, ds: pd.Series) -> pd.Series:
        date = localdate("Brazil/East")
        return enrich_data(ds, "date", date)

    def pipe_location(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "location", self.location)

    def pipe_vaccine(self, ds: pd.Series) -> pd.Series:
        return enrich_data(
            ds,
            "vaccine",
            "Johnson&Johnson, Pfizer/BioNTech, Oxford/AstraZeneca, Sinovac",
        )

    def pipe_source(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "source_url", self.source_url)

    def pipeline(self, ds: pd.Series) -> pd.Series:
        return ds.pipe(self.pipe_date).pipe(self.pipe_location).pipe(self.pipe_vaccine).pipe(self.pipe_source)

    def export(self, paths):
        data = self.read().pipe(self.pipeline)
        increment(
            paths=paths,
            location=data["location"],
            total_vaccinations=data["total_vaccinations"],
            people_vaccinated=data["people_vaccinated"],
            people_fully_vaccinated=data["people_fully_vaccinated"],
            date=data["date"],
            source_url=data["source_url"],
            vaccine=data["vaccine"],
        )


def main(paths):
    Brazil().export(paths)
