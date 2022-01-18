import pandas as pd

from cowidev.utils.web.scraping import get_soup, request_text
from cowidev.utils.clean import extract_clean_date
from cowidev.testing.utils.incremental import increment


class SouthKorea:
    location = "South Korea"
    units = "people tested"
    source_label = "Ministry of Health"
    source_url = (
        "http://ncov.mohw.go.kr/en/bdBoardList.do?brdId=16&brdGubun=161&dataGubun=&ncvContSeq=&contSeq=&board_id="
    )
    notes = ""

    def read(self):
        return {
            "daily_change": self._parse_metric(),
            "date": self._parse_date(),
        }

    def _parse_metric(self):
        text = request_text(self.source_url, mode="raw")
        table = pd.read_html(text, index_col=0)[6]
        daily_change = int(table["Total"])
        return daily_change

    def _parse_date(self):
        soup = get_soup(self.source_url)
        date_raw = soup.select_one(".t_date").text
        dt_raw = extract_clean_date(date_raw, regex="(\\d.\\d\\d)", date_format="%f.%d", replace_year=2022)
        return dt_raw

    def export(self):
        data = self.read()
        increment(
            daily_change=data["daily_change"],
            sheet_name=self.location,
            country=self.location,
            units=self.units,
            date=data["date"],
            source_url=self.source_url,
            source_label=self.source_label,
        )


def main():
    SouthKorea().export()


if __name__ == "__main__":
    main()
