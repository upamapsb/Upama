import re

from selenium.webdriver.chrome.webdriver import WebDriver
from selenium.webdriver.remote.webelement import WebElement
import pandas as pd

from cowidev.utils.web import get_driver
from cowidev.utils.clean import clean_count, clean_date
from cowidev.vax.utils.incremental import increment, enrich_data


class Iran:
    location = "Iran"
    _base_url = "https://behdasht.gov.ir"
    _url_subdirectory = "اخبار/صفحه"
    _num_max_pages = 3
    regex = {
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

        # driver = get_driver()

        with get_driver() as driver:
            for cnt in range(1, self._num_max_pages + 1):
                url = f"{self._base_url}/{self._url_subdirectory}:{cnt}"
                driver.get(url)
                data, proceed = self._parse_data(driver)
                if not proceed:
                    break
        # driver.quit()

        return pd.Series(data)

    # def _get_news_links(self, driver: WebDriver):

    def _parse_data(self, driver: WebDriver) -> tuple:
        """Get data from driver current page."""
        # Get relevant element
        element = self._get_relevant_element(driver)
        if not element:
            return None, True
        # Extract link and date from link
        link, date = self._get_link_and_date_from_element(element)
        # driver.quit()
        # driver = get_driver()
        driver.get(link)
        text = driver.find_element_by_class_name("news-content").text
        record = {
            "source_url": link,
            "date": date,
            **self._parse_metrics(text),
        }
        return record, False

    def _get_relevant_element(self, driver: WebDriver):
        """Get relevant element in news feed."""
        news_list = driver.find_elements_by_class_name("es-post-dis")

        url_idx = [
            i
            for i, news in enumerate(news_list)
            if re.search(self.regex["summary"], news.text)
            and (not re.search(self.regex["exclude_summary"], news.text))
        ]
        if not url_idx:
            return None
        return news_list[url_idx[0]]

    def _get_link_and_date_from_element(self, elem: WebElement) -> dict:
        """Extract link and date from relevant element."""
        link = self._parse_link_from_element(elem)
        if not link:
            return None
        date = self._parse_date_from_element(elem)
        return link, date

    def _parse_date_from_element(self, elem: WebElement) -> str:
        """Get date from relevant element."""
        date = elem.find_element_by_tag_name("li").text
        date_list = re.search(r"(\d+\/\d+\/\d+)", date).group().split("/")
        return clean_date(date_converter(date_list), "%Y-%m-%d")

    def _parse_link_from_element(self, elem: WebElement):
        """Get link from relevant element."""
        href = elem.find_element_by_tag_name("a").get_attribute("href")
        return href

    def _parse_metrics(self, text: str) -> dict:
        """Get metrics from news text."""
        people_vaccinated = numeric_word_converter(re.search(self.regex["people_vaccinated"], text).group(1))
        people_fully_vaccinated = numeric_word_converter(
            re.search(self.regex["people_fully_vaccinated"], text).group(1)
        )
        total_boosters = numeric_word_converter(re.search(self.regex["total_boosters"], text).group(1))
        total_vaccinations = numeric_word_converter(re.search(self.regex["total_vaccinations"], text).group(1))
        return {
            "people_vaccinated": clean_count(people_vaccinated),
            "people_fully_vaccinated": clean_count(people_fully_vaccinated),
            "total_boosters": clean_count(total_boosters),
            "total_vaccinations": clean_count(total_vaccinations),
        }

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


def numeric_word_converter(numeric_word: str) -> int:
    numwords = {}
    # 0 - 9 digits
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


def date_converter(j_date: list[str]) -> str:
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
