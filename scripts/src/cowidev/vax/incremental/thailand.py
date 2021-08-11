import re
import requests
from datetime import datetime
import tempfile

from bs4 import BeautifulSoup
import pandas as pd
from pdfreader import SimplePDFViewer

from cowidev.vax.utils.incremental import (
    clean_count,
    merge_with_current_data,
)
from cowidev.vax.utils.utils import get_soup
from cowidev.vax.utils.dates import clean_date


class Thailand:
    def __init__(self):
        self.location = "Thailand"
        self.source_url = "https://ddc.moph.go.th/dcd/pagecontent.php?page=643&dept=dcd"
        self.regex_date = r"\s?ข้อมูล ณ วันที่ (\d{1,2}) (.*) (\d{4})"

    @property
    def regex_vax(self):
        regex_aux = r"\((?:รา|รำ)ย\)"
        regex_vax = (
            r" ".join([f"เข็มที่ {i} {regex_aux}" for i in range(1, 4)])
            + r" รวม \(โดส\) ([\d,]+) ([\d,]+) ([\d,]+) ([\d,]+)"
        )
        return regex_vax

    def read(self, last_update: str) -> pd.DataFrame:
        yearly_report_page = get_soup(self.source_url)
        # Get Newest Month Report Page
        monthly_report_link = yearly_report_page.find(
            "div", class_="col-lg-12", id="content-detail"
        ).find("a")["href"]
        monthly_report_page = get_soup(monthly_report_link)
        # Get links
        df = self._parse_data(monthly_report_page, last_update)
        return df

    def _parse_data(self, monthly_report_page: BeautifulSoup, last_update: str):
        elems = monthly_report_page.find("div", class_="col-lg-12", id="content-detail")
        links = [link["href"].replace(" ", "%20") for link in elems.find_all("a")]
        records = []
        for link in links:
            # print("-------------------")
            # print(link)
            record, stop = self._parse_data_date(link, last_update)
            if stop:
                break
            records.append(record)
        return pd.DataFrame(records)

    def _parse_data_date(self, link: str, last_update: str) -> dict:
        # Get text from PDF
        raw_text = self._text_from_pdf(link)
        text = self._substitute_special_chars(raw_text)
        # Get date
        date_str = self._parse_date(text)
        if date_str < last_update:
            return None, True
        data = self._parse_metrics(text)
        data["date"] = date_str
        data["source_url"] = link.replace(" ", "%20")
        return data, False

    def _text_from_pdf(self, pdf_link: str):
        with tempfile.NamedTemporaryFile() as tf:
            with open(tf.name, mode="wb") as f:
                f.write(requests.get(pdf_link).content)

            with open(tf.name, mode="rb") as f:
                viewer = SimplePDFViewer(f)
                viewer.render()
                raw_text = "".join(viewer.canvas.strings)
        return raw_text

    def _substitute_special_chars(self, raw_text: str):
        """Correct Thai Special Character Error."""
        special_char_replace = {
            "\uf701": "\u0e34",
            "\uf702": "\u0e35",
            "\uf703": "\u0e36",
            "\uf704": "\u0e37",
            "\uf705": "\u0e48",
            "\uf706": "\u0e49",
            "\uf70a": "\u0e48",
            "\uf70b": "\u0e49",
            "\uf70e": "\u0e4c",
            "\uf710": "\u0e31",
            "\uf712": "\u0e47",
            "\uf713": "\u0e48",
            "\uf714": "\u0e49",
        }
        special_char_replace = dict(
            (re.escape(k), v) for k, v in special_char_replace.items()
        )
        pattern = re.compile("|".join(special_char_replace.keys()))
        text = pattern.sub(
            lambda m: special_char_replace[re.escape(m.group(0))], raw_text
        )
        return text

    def _parse_metrics(self, text: str):
        metrics = re.search(self.regex_vax, text).groups()
        people_vaccinated = clean_count(metrics[0])
        people_fully_vaccinated = clean_count(metrics[1])
        total_boosters = clean_count(metrics[2])
        total_vaccinations = clean_count(metrics[3])
        return {
            "total_vaccinations": total_vaccinations,
            "people_vaccinated": people_vaccinated,
            "people_fully_vaccinated": people_fully_vaccinated,
            "total_boosters": total_boosters,
        }

    def _parse_date(self, text: str):
        thai_date_replace = {
            # Months
            "มกราคม": 1,
            "กุมภาพันธ์": 2,
            "มีนาคม": 3,
            "เมษายน": 4,
            "พฤษภาคม": 5,
            "พฤษภำคม": 5,
            "มิถุนายน": 6,
            "มิถุนำยน": 6,
            "กรกฎาคม": 7,
            "กรกฎำคม": 7,
            "สิงหาคม": 8,
            "สิงหำคม": 8,
            "กันยายน": 9,
            "ตุลาคม": 10,
            "พฤศจิกายน": 11,
            "ธันวาคม": 12,
        }
        date_raw = re.search(self.regex_date, text)
        day = clean_count(date_raw.group(1))
        month = thai_date_replace[date_raw.group(2)]
        year = clean_count(date_raw.group(3)) - 543
        return clean_date(datetime(year, month, day))

    def pipe_location(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(location=self.location)

    def pipe_vaccine(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(vaccine="Oxford/AstraZeneca, Sinovac")

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.pipe(self.pipe_location).pipe(self.pipe_vaccine)

    def to_csv(self, paths):
        output_file = paths.tmp_vax_out(self.location)
        last_update = pd.read_csv(output_file).date.max()
        df = self.read(last_update)
        if not df.empty:
            df = df.pipe(self.pipeline)
            df = merge_with_current_data(df, output_file)
            df.to_csv(output_file, index=False)


def main(paths):
    Thailand().to_csv(paths)


if __name__ == "__main__":
    main()
