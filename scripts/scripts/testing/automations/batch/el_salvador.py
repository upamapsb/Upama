import datetime

import pandas as pd

from cowidev.testing import CountryTestBase
from cowidev.utils import get_soup, clean_count
from cowidev.utils.clean.dates import clean_date, localdate


class ElSalvador(CountryTestBase):
    location = "El Salvador"
    units = "tests performed"
    source_label = "Government of El Salvador"

    def read(self):
        df = self.load_datafile()
        date = clean_date(df.Date.max(), "%Y-%m-%d", as_datetime=True)
        end_date = localdate(None, force_today=True, as_datetime=True)
        records = []
        while date < end_date:
            print(date)
            source_url = f"https://diario.innovacion.gob.sv/?fechaMostrar={date.strftime('%d-%m-%Y')}"
            soup = get_soup(source_url)
            daily = clean_count(
                soup.find("div", class_="col-4 col-sm-2 col-lg-2 align-self-center offset-lg-0")
                .find("label")
                .text.strip()
            )
            records.append(
                {
                    "Date": [clean_date(date, "%Y-%m-%d")],
                    "Daily change in cumulative total": daily,
                    "Source URL": source_url,
                }
            )
            # increment
            date += datetime.timedelta(days=1)

        # Build dataframe
        df_new = pd.DataFrame.from_records(records)
        df_new = df_new.pipe(self.pipe_metadata)
        df = pd.concat([df, df_new])
        df = df.drop_duplicates().dropna(subset=["Daily change in cumulative total"])
        self.export_datafile(df)


def main():
    ElSalvador().read()


if __name__ == "__main__":
    main()
