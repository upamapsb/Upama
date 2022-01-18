import pandas as pd

from cowidev.utils.clean.dates import localdate
from cowidev.utils.web.scraping import get_soup
from cowidev.vax.utils.incremental import enrich_data, increment


class Japan:
    location: str = "Japan"
    source_url: str = "https://www.kantei.go.jp/jp/headline/kansensho/vaccine.html"

    def read(self) -> pd.Series:
        return self._parse_data()

    def _parse_data(self) -> pd.Series:
        soup = get_soup(self.source_url)
        metrics = self._parse_metrics(soup)
        data = {
            **metrics,
            "date": self._parse_date(soup),
        }
        return pd.Series(data=data)

    def _parse_metrics(self, soup):
        df = pd.read_html(str(soup.find(class_="vaccination-count")))[0]
        assert df.shape == (4, 7)

        values = df.iloc[:, 2].values
        total_vaccinations = values[0]
        people_vaccinated = values[1]
        people_fully_vaccinated = values[2]
        total_boosters = values[3]
        assert total_vaccinations == people_vaccinated + people_fully_vaccinated + total_boosters
        return {
            "total_vaccinations": total_vaccinations,
            "people_vaccinated": people_vaccinated,
            "people_fully_vaccinated": people_fully_vaccinated,
            "total_boosters": total_boosters,
        }

    def _parse_date(self, soup):
        date = soup.find(class_="aly_tx_center").text
        date = localdate("Asia/Tokyo")
        return date

    def pipe_location(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "location", self.location)

    def pipe_vaccine(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "vaccine", "Moderna, Pfizer/BioNTech, Oxford/AstraZeneca")

    def pipe_source(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "source_url", self.source_url)

    def pipeline(self, ds: pd.Series) -> pd.Series:
        return ds.pipe(self.pipe_location).pipe(self.pipe_vaccine).pipe(self.pipe_source)

    def export(self):
        data = self.read().pipe(self.pipeline)
        increment(
            location=data["location"],
            total_vaccinations=data["total_vaccinations"],
            people_vaccinated=data["people_vaccinated"],
            people_fully_vaccinated=data["people_fully_vaccinated"],
            total_boosters=data["total_boosters"],
            date=data["date"],
            source_url=data["source_url"],
            vaccine=data["vaccine"],
        )


def main():
    Japan().export()


if __name__ == "__main__":
    main()
