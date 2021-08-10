import time

import pandas as pd

from cowidev.vax.utils.incremental import enrich_data, increment, clean_count
from cowidev.vax.utils.utils import get_driver
from cowidev.vax.utils.dates import clean_date


class Philippines:

    def __init__(self) -> None:
        self.location = "Philippines"
        self.source_url = "https://e.infogram.com/_/yFVE69R1WlSdqY3aCsBF"
        self.source_url_ref = (
            "https://news.abs-cbn.com/spotlight/multimedia/infographic/03/23/21/philippines-covid-19-vaccine-tracker"
        )

    def read(self) -> pd.Series:
        return pd.Series(
            data=self._parse_data()
        )

    def _parse_data(self) -> dict:
        with get_driver() as driver:
            driver.get(self.source_url)
            time.sleep(2)
            spans = [
                span
                for span in driver.find_elements_by_tag_name("span")
                if span.get_attribute("data-text")
            ]
            # Date
            date = clean_date(spans[6].text, "(as of %b. %d, %Y)")
            # Metrics
            total_vaccinations = clean_count(spans[8].text)
            people_vaccinated = clean_count(spans[13].text)
            people_fully_vaccinated = clean_count(spans[14].text)
        # Sanity check
        if total_vaccinations != people_vaccinated + people_fully_vaccinated:
            raise ValueError("total_vaccinations should equal sum of first and second doses.")

        return {
            "total_vaccinations": total_vaccinations,
            "people_vaccinated": people_vaccinated,
            "people_fully_vaccinated": people_fully_vaccinated,
            "date": date,
        }

    def pipe_location(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "location", self.location)

    def pipe_vaccine(self, ds: pd.Series) -> pd.Series:
        return enrich_data(
            ds,
            "vaccine",
            "Johnson&Johnson, Moderna, Oxford/AstraZeneca, Pfizer/BioNTech, Sinovac, Sputnik V",
        )

    def pipe_source(self, ds: pd.Series) -> pd.Series:
        return enrich_data(
            ds,
            "source_url",
            self.source_url_ref,
        )

    def pipeline(self, ds: pd.Series) -> pd.Series:
        return (
            ds
            .pipe(self.pipe_location).
            pipe(self.pipe_vaccine)
            .pipe(self.pipe_source)
        )

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
    Philippines().export(paths)
