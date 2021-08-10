from datetime import date, timedelta

import pandas as pd


URL = "https://opendata.ecdc.europa.eu/covid19/vaccine_tracker/csv/data.csv"
VACCINES_ONE_DOSE = ["JANSS"]


def main(paths):

    df = pd.read_csv(
        URL, usecols=["YearWeekISO", "FirstDose", "SecondDose", "Region", "Vaccine"]
    )

    df = df[df.Region == "NL"]

    df = df.rename(
        columns={
            "YearWeekISO": "date",
            "FirstDose": "people_vaccinated",
            "SecondDose": "people_fully_vaccinated",
            "Region": "location",
        }
    )

    # Calculate metrics
    df = df.assign(total_vaccinations=df.people_vaccinated + df.people_fully_vaccinated)
    df.loc[
        df.Vaccine.isin(VACCINES_ONE_DOSE), "people_fully_vaccinated"
    ] = df.people_vaccinated
    df = df.drop(columns="Vaccine").groupby("date").sum().cumsum().reset_index()

    # Convert week numbers to dates (Sunday of each week)
    # For the current week, the date is set to yesterday.
    df = df[df.date.str[:4] == "2021"]
    df["week_number"] = df.date.str[-2:].astype(int)
    df["date"] = pd.to_datetime("2021-01-03")
    df["date"] = (df.date + df.week_number * pd.DateOffset(days=7)).dt.date
    df.loc[df.date >= date.today(), "date"] = date.today() - timedelta(days=1)

    df = df.drop(columns="week_number")
    df = df.assign(
        location="Netherlands",
        source_url="https://www.ecdc.europa.eu/en/publications-data/data-covid-19-vaccination-eu-eea",
    )
    df = df.pipe(enrich_vaccine_name)
    df.to_csv(paths.tmp_vax_out("Netherlands"), index=False)


def enrich_vaccine_name(df: pd.DataFrame) -> pd.DataFrame:
    def _enrich_vaccine_name(dt: str) -> str:
        # See timeline in:
        if dt < date(2021, 1, 18):
            return "Pfizer/BioNTech"
        elif date(2021, 1, 18) <= dt < date(2021, 2, 10):
            return "Moderna, Pfizer/BioNTech"
        elif date(2021, 2, 10) <= dt < date(2021, 4, 21):
            return "Moderna, Oxford/AstraZeneca, Pfizer/BioNTech"
        elif date(2021, 4, 21) <= dt:
            return "Johnson&Johnson, Moderna, Oxford/AstraZeneca, Pfizer/BioNTech"

    return df.assign(vaccine=df.date.apply(_enrich_vaccine_name))


if __name__ == "__main__":
    main()
