import re

from selenium.webdriver.chrome.webdriver import WebDriver
from selenium.webdriver.remote.webelement import WebElement
import pandas as pd

from cowidev.utils.web import get_driver
from cowidev.utils.clean import clean_count, clean_date
from cowidev.vax.utils.incremental import increment, enrich_data


class Iran:
    def __init__(self):
        self.location = "Iran"
        self._base_url = "https://behdasht.gov.ir"
        self._url_subdirectory = "اخبار/صفحه"
        self._num_max_pages = 3
        self.regex = {
            "summary": r"مرکز روابط عمومی (.*?) ",
            "exclude_summary": r"مرکز روابط عمومی (.*?) گفت",
            "date": r"(\d+\/\d+\/\d+)",
            "people_vaccinated": r"کنون (.*?) نفر دُز اول",
            "people_fully_vaccinated": r"اول،(.*?)نفر دُز دوم",
            "total_boosters": r"دوم و (.*?)نفر، دُز سوم",
            "total_vaccinations": r"در کشور به (.*?) دُز",
        }

    def read(self) -> pd.Series:
        data = []
        driver = get_driver()

        for cnt in range(1, self._num_max_pages + 1):
            url = f"{self._base_url}/{self._url_subdirectory}:{cnt}"
            driver.get(url)
            data, proceed = self.parse_data(driver)
            if not proceed:
                break
        
        driver.quit()

        return pd.Series(data)

    def parse_data(self, driver: WebDriver) -> tuple:

        news_list = driver.find_elements_by_class_name("es-post-dis")

        indices_to_be_deleted = [
            i for i, news in enumerate(news_list) if re.search(self.regex["exclude_summary"], news.text)
        ]
        accuired_indices = [i for i, news in enumerate(news_list) if re.search(self.regex["summary"], news.text)]
        url_idx = [index for index in accuired_indices if index not in indices_to_be_deleted]

        if url_idx is None:
            return None, True

        elem = self.get_link_and_date(news_list[url_idx[0]])

        driver.quit()
        driver = get_driver()

        driver.get(elem["link"])
        text = driver.find_element_by_class_name("news-content").text

        record = {
            "source_url": elem["link"],
            "date": elem["date"],
            **self.parse_data_news_page(text),
        }
        return record, False

    def get_link_and_date(self, elem: WebElement) -> dict:
        link = self.parse_link(elem)
        date = elem.find_element_by_tag_name("li").text
        if not link:
            return None
        link_date = {"link": link, "date": self.parse_date(date)}
        return link_date

    def parse_date(self, date: str) -> str:
        date_list = re.search(r"(\d+\/\d+\/\d+)", date).group().split("/")
        return clean_date(self.date_converter(date_list), "%Y-%m-%d")

    def parse_link(self, elem: WebElement):
        href = elem.find_element_by_tag_name("a").get_attribute("href")
        return href

    def parse_data_news_page(self, text: str) -> dict:
        people_vaccinated = self.numeric_word_converter(re.search(self.regex["people_vaccinated"], text).group(1))
        people_fully_vaccinated = self.numeric_word_converter(
            re.search(self.regex["people_fully_vaccinated"], text).group(1)
        )
        total_boosters = self.numeric_word_converter(re.search(self.regex["total_boosters"], text).group(1))
        total_vaccinations = self.numeric_word_converter(
            re.search(self.regex["total_vaccinations"], text).group(1)
        )
        return {
            "people_vaccinated": clean_count(people_vaccinated),
            "people_fully_vaccinated": clean_count(people_fully_vaccinated),
            "total_boosters": clean_count(total_boosters),
            "total_vaccinations": clean_count(total_vaccinations),
        }

    def numeric_word_converter(self, numeric_word: str) -> int:

        numwords = {}

        digits = {
            "۰": "0",
            "۱": "1",
            "۲": "2",
            "۳": "3",
            "۴": "4",
            "۵": "5",
            "۶": "6",
            "۷": "7",
            "۸": "8",
            "۹": "9",
        }
        units = [
            "صفر",
            "یک",
            "دو",
            "سه",
            "چهار",
            "پنج",
            "شش",
            "هفت",
            "هشت",
            "نه",
            "ده",
            "یازده",
            "دوازده",
            "سیزده",
            "چهارده",
            "پانزده",
            "شانزده",
            "هفده",
            "هجده",
            "نوزده",
        ]

        tens = ["", "", "بیست", "سی", "چهل", "پنجاه", "شصت", "هفتاد", "هشتاد", "نود"]

        hundreds = [
            "",
            "یكصد",
            "دویست",
            "سیصد",
            "چهارصد",
            "پانصد",
            "ششصد",
            "هفتصد",
            "هشتصد",
            "نهصد",
        ]

        scales = ["صد", "هزار", "میلیون", "میلیارد"]

        numwords["و"] = (1, 0)

        for idx, word in enumerate(units):
            numwords[word] = (1, idx)
        for idx, word in enumerate(tens):
            numwords[word] = (1, idx * 10)
        for idx, word in enumerate(hundreds):
            numwords[word] = (1, idx * 100)
        for idx, word in enumerate(scales):
            numwords[word] = (10 ** (idx * 3 or 2), 0)

        current = result = 0

        numeric_word = re.sub(r"[\u06F0-\u06F9]", lambda m: digits[m.group()], numeric_word)

        for word in numeric_word.replace("-", " ").split():

            if word not in numwords and not word.isnumeric():
                raise Exception("Illegal word: " + word)

            elif word.isnumeric():
                scale = 1
                increment = int(word)
            else:
                scale, increment = numwords[word]

            current = current * scale + increment

            if scale > 100:
                result += current
                current = 0

        return int(result + current)

    def date_converter(self, j_date: list[str]) -> str:
        """Convert jalali date to gregorian date

        Args:
            j_date list[str]: Jalali date in format: [YYYY,mm,dd]

        return:
            str: Gregorian date in format: YYYY-mm-dd
        """

        j_year = int(j_date[0])
        j_month = int(j_date[1])
        j_day = int(j_date[2])

        j_year += 1595
        days_total = -355668 + (365 * j_year) + ((j_year // 33) * 8) + (((j_year % 33) + 3) // 4) + j_day

        if j_month < 7:
            days_total += (j_month - 1) * 31
        else:
            days_total += ((j_month - 7) * 30) + 186

        g_year = 400 * (days_total // 146097)
        days_total %= 146097

        if days_total > 36524:

            days_total -= 1
            g_year += 100 * (days_total // 36524)
            days_total %= 36524

            if days_total >= 365:
                days_total += 1

        g_year += 4 * (days_total // 1461)
        days_total %= 1461

        if days_total > 365:

            g_year += (days_total - 1) // 365
            days = (days_total - 1) % 365

        g_day = days + 1

        if (g_year % 4 == 0 and g_year % 100 != 0) or (g_year % 400 == 0):
            feb = 29
        else:
            feb = 28
        g_months = [0, 31, feb, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
        g_month = 0

        while g_month < 13 and g_day > g_months[g_month]:
            g_day -= g_months[g_month]
            g_month += 1

        g_date = str(g_year) + "-" + str(g_month) + "-" + str(g_day)

        return g_date

    def pipe_location(self, ds: pd.Series) -> pd.Series:
        return enrich_data(ds, "location", self.location)

    def pipe_vaccine(self, ds: pd.Series) -> pd.Series:
        return enrich_data(
            ds,
            "vaccine",
            "COVIran Barekat, Covaxin, Oxford/AstraZeneca, Sinopharm/Beijing, Soberana02, Sputnik V",
        )

    def pipeline(self, ds: pd.Series) -> pd.Series:
        return ds.pipe(self.pipe_location).pipe(self.pipe_vaccine)

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
    Iran().export()


if __name__ == "__main__":
    main()
