from bs4 import BeautifulSoup

import pandas as pd
import requests
import json
import re

from cowidev.utils.clean.dates import localdate
from cowidev.utils.web.scraping import get_soup
from cowidev.vax.utils.incremental import enrich_data, increment


class Indonesia:
    location: str = "Indonesia"
    source_url_ref: str = "https://vaksin.kemkes.go.id/#/vaccines"
    source_url_dose_1: str = (
        "https://public.tableau.com/views/DashboardVaksinKemkes/TotalVaksinasiDosis1?:embed=yes&:showVizHome=no"
    )
    source_url_dose_2: str = (
        "https://public.tableau.com/views/DashboardVaksinKemkes/TotalVaksinasiDosis2?:embed=yes&:showVizHome=no"
    )

    def read(self) -> pd.Series:
        dose1_soup = get_soup(self.source_url_dose_1)
        dose2_soup = get_soup(self.source_url_dose_2)
        return self._parse_data(dose1_soup, dose2_soup)

    def _parse_data(self, dose1_soup: BeautifulSoup, dose2_soup: BeautifulSoup) -> pd.Series:
        dose1 = self._parse_tableau(dose1_soup)
        dose2 = self._parse_tableau(dose2_soup)
        data = pd.Series(
            {
                "people_vaccinated": dose1,
                "people_fully_vaccinated": dose2,
                "total_vaccinations": dose1 + dose2,
            }
        )
        return data

    def _parse_tableau(self, soup: BeautifulSoup) -> int:
        tableauData = json.loads(soup.find("textarea", {"id": "tsConfigContainer"}).text)
        dataUrl = (
            f'https://public.tableau.com{tableauData["vizql_root"]}/bootstrapSession/sessions/'
            f'{tableauData["sessionid"]}'
        )
        r = requests.post(dataUrl, data={"sheet_id": tableauData["sheetId"]})
        dataReg = re.search(r"\d+;({.*})\d+;({.*})", r.text, re.MULTILINE)
        data = json.loads(dataReg.group(2))
        return data["secondaryInfo"]["presModelMap"]["dataDictionary"]["presModelHolder"][
            "genDataDictionaryPresModel"
        ]["dataSegments"]["0"]["dataColumns"][0]["dataValues"][0]

    def pipe_date(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "date", localdate("Asia/Jakarta", force_today=True))

    def pipe_location(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "location", self.location)

    def pipe_vaccine(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "vaccine", "Moderna, Oxford/AstraZeneca, Pfizer/BioNTech, Sinopharm/Beijing, Sinovac")

    def pipe_source(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "source_url", self.source_url_ref)

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
    Indonesia().export(paths)
