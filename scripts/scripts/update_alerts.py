import argparse
import datetime

import pandas as pd

parser = argparse.ArgumentParser()
parser.add_argument("--data", type=str, help="Type of COVID-19 data. One of: [jhu, vax, testing]")
args = parser.parse_args()

JHU_URL = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/jhu/full_data.csv"
VAX_URL = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/vaccinations/vaccinations.csv"
TESTING_URL = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/testing/covid-testing-all-observations.csv"


def check_updated(url, date_col, allowed_days) -> None:
    df = pd.read_csv(url)
    max_date = df[date_col].max()
    if max_date < str(datetime.date.today() - datetime.timedelta(days=allowed_days)):
        raise Exception(
            f"{args.data.upper()} COVID-19 data is not updated! "
            "Check if something is broken in our pipeline and/or if someone is in charge of today's update."
        )
    else:
        print("Check passed. All good!")


def main():
    if args.data == "jhu":
        check_updated(JHU_URL, "date", allowed_days=1)
    elif args.data == "vax":
        check_updated(VAX_URL, "date", allowed_days=1)
    elif args.data == "testing":
        check_updated(TESTING_URL, "Date", allowed_days=7)
    else:
        raise Exception("Wrong data type used! Use one of: [jhu, vax, testing]")


if __name__ == "__main__":
    main()
