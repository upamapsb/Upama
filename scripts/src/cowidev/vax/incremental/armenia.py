import re
import datetime
import warnings
import pandas as pd

from facebook_scraper import get_posts

from cowidev.utils.clean import clean_count
from cowidev.vax.utils.incremental import merge_with_current_data


class Armenia:
    def __init__(self, page_id: str = "ministryofhealthcare", location: str = "Armenia"):
        self.page_id = page_id
        self.location = location
        self._options = {
            "posts_per_page": 25,
            "allow_extra_requests": False,
        }

        self.months = [
            "հունվար",
            "փետրվար",
            "մարտ",
            "ապրիլ",
            "մայիս",
            "հունիս",
            "հուլիս",
            "օգոստոս",
            "սեպտեմբեր",
            "հոկտեմբեր",
            "նոյեմբեր",
            "դեկտեմբեր",
        ]
        self.month_lookup = dict([(m, i + 1) for i, m in enumerate(self.months)])
        self.regex = {
            "title": r"^Պատվաստումային գործընթացը շարունակվում է\n\nCOVID-19-ի դեմ պատվաստումների մեկնարկից ի վեր",
            "date": r"(" + "|".join(self.months) + r")ի (\d{1,2})-ի դրությամբ`",
            "total_vaccinations": r"կատարվել է ([\d,]+) պատվաստում",
            "people_vaccinated": r"առաջին դեղաչափ` ([\d,]+)",
            "people_fully_vaccinated": r"երկրորդ դեղաչափ` ([\d,]+)",
            "source_url": r"https://www.moh.am/#1/([\d,]+)",
        }

    def read(self, last_update: str) -> pd.DataFrame:
        last_update = datetime.date.fromisoformat(last_update)  # must be %Y-%m-%d

        data = []
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")

            for post in get_posts(self.page_id, pages=10, options=self._options):
                if post["time"].date() <= last_update:
                    break
                if self.is_vaccination_update(post):
                    data.append(self.parse_vaccination_post(post))

        return pd.DataFrame(data)

    def is_vaccination_update(self, post):
        return post["text"] and re.match(self.regex["title"], post["text"])

    def parse_vaccination_post(self, post):
        record = {}

        date = re.search(self.regex["date"], post["text"])
        if date:
            year = post["time"].year
            month = self.month_lookup[date.group(1)]
            day = int(date.group(2))
            record["date"] = datetime.date(year, month, day).isoformat()

        total_vaccinations = re.search(self.regex["total_vaccinations"], post["text"])
        if total_vaccinations:
            record["total_vaccinations"] = clean_count(total_vaccinations.group(1))

        people_vaccinated = re.search(self.regex["people_vaccinated"], post["text"])
        if people_vaccinated:
            record["people_vaccinated"] = clean_count(people_vaccinated.group(1))

        people_fully_vaccinated = re.search(self.regex["people_fully_vaccinated"], post["text"])
        if people_fully_vaccinated:
            record["people_fully_vaccinated"] = clean_count(people_fully_vaccinated.group(1))

        source_url = re.search(self.regex["source_url"], post["text"])
        if source_url:
            record["source_url"] = source_url.group(0)
        else:
            record["source_url"] = post["post_url"]

        return record

    def pipe_drop_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.sort_values("date").drop_duplicates(
            subset=[
                "total_vaccinations",
                "people_vaccinated",
                "people_fully_vaccinated",
            ],
            keep="first",
        )

    def pipe_location(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(location=self.location)

    def pipe_vaccine(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(vaccine="Oxford/AstraZeneca, Sinopharm/Beijing, Sinovac, Sputnik V")

    def pipe_select_output_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return df[
            [
                "location",
                "date",
                "vaccine",
                "source_url",
                "total_vaccinations",
                "people_vaccinated",
                "people_fully_vaccinated",
            ]
        ]

    def pipeline(self, df: pd.Series) -> pd.Series:
        return (
            df.pipe(self.pipe_drop_duplicates)
            .pipe(self.pipe_location)
            .pipe(self.pipe_vaccine)
            .pipe(self.pipe_select_output_columns)
            .sort_values(by="date")
        )

    def to_csv(self, paths):
        """Generalized."""
        output_file = paths.tmp_vax_out(self.location)
        last_update = pd.read_csv(output_file).date.max()
        df = self.read(last_update)
        if not df.empty:
            df = df.pipe(self.pipeline)
            df = merge_with_current_data(df, output_file)
            df = df.pipe(self.pipe_drop_duplicates)
            df.to_csv(output_file, index=False)


def main(paths):
    Armenia().to_csv(paths)


if __name__ == "__main__":
    main()
