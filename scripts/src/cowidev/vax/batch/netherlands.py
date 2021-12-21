import epiweeks
import pandas as pd

from cowidev.utils import paths
from cowidev.vax.utils.utils import build_vaccine_timeline


SOURCE = "https://github.com/mzelst/covid-19/raw/master/data-rivm/vaccines-ecdc/vaccines_administered_nl.csv"
SOURCE_REF = "https://github.com/mzelst/covid-19"

ONE_DOSE_VACCINES = ["Johnson&Johnson"]


def week_to_date(row):
    week = epiweeks.Week(row.year, row.week)
    return str(week.enddate())


def main():
    df = pd.read_csv(SOURCE)

    df = df[df.total_administered > 0]

    df = df.assign(date=df.apply(week_to_date, axis=1)).drop(columns=["week", "year"])

    # Vaccine timeline
    timeline = df[df.vaccine != "UNK"][["vaccine", "date"]].groupby("vaccine").min().to_dict()["date"]

    # Total vaccinations
    df["total_vaccinations"] = df.total_administered

    # People vaccinated
    df.loc[df.dose_number == 1, "people_vaccinated"] = df.total_administered

    # People fully vaccinated
    df.loc[
        (df.dose_number == 2) & (-df.vaccine.isin(ONE_DOSE_VACCINES)), "people_fully_vaccinated"
    ] = df.total_administered
    df.loc[
        (df.dose_number == 1) & (df.vaccine.isin(ONE_DOSE_VACCINES)), "people_fully_vaccinated"
    ] = df.total_administered

    # Boosters
    df.loc[(df.dose_number > 2) & (-df.vaccine.isin(ONE_DOSE_VACCINES)), "total_boosters"] = df.total_administered
    df.loc[(df.dose_number > 1) & (df.vaccine.isin(ONE_DOSE_VACCINES)), "total_boosters"] = df.total_administered

    df = (
        df.drop(columns=["dose_number", "total_administered", "vaccine"])
        .fillna(0)
        .groupby("date", as_index=False)
        .sum()
        .sort_values("date")
        .assign(location="Netherlands", source_url=SOURCE_REF)
    )

    df[["people_vaccinated", "people_fully_vaccinated", "total_vaccinations", "total_boosters"]] = df[
        ["people_vaccinated", "people_fully_vaccinated", "total_vaccinations", "total_boosters"]
    ].cumsum()

    df = build_vaccine_timeline(df, timeline)

    df.to_csv(paths.out_vax("Netherlands"), index=False)


if __name__ == "__main__":
    main()
