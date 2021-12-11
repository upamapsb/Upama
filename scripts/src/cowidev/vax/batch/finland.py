import datetime
import re

import pandas as pd

from cowidev.utils import paths
from cowidev.vax.utils.utils import build_vaccine_timeline

ALL_DOSES = "https://sampo.thl.fi/pivot/prod/en/vaccreg/cov19cov/fact_cov19cov.csv?&row=dateweek20201226-525467.525455.525460.525438.525454.525462.525441.527848.531437.533036.533161.533828.533893.537676.539117.547307.547361.547509.547559.566011.567632.568219.580342.580442.581008.581100.590261.600765.601510.601865.601960.601981.602272.602655.610782.610832.617752.618236.619370.627523.628801.629136.629319.630310.639251.639303.642077.642795.650935.651376.651491.&column=cov_vac_age-590256.610735.610734.533184.533177.533181.533162.533171.533169.533182.533176.533173.533183.533179.533178.533166.&column=measure-533175.&filter=area-518362.&filter=cov_vac_dose-533174."
FIRST_DOSES = "https://sampo.thl.fi/pivot/prod/en/vaccreg/cov19cov/fact_cov19cov.csv?&row=dateweek20201226-525467.525455.525460.525438.525454.525462.525441.527848.531437.533036.533161.533828.533893.537676.539117.547307.547361.547509.547559.566011.567632.568219.580342.580442.581008.581100.590261.600765.601510.601865.601960.601981.602272.602655.610782.610832.617752.618236.619370.627523.628801.629136.629319.630310.639251.639303.642077.642795.650935.651376.651491.&column=cov_vac_age-590256.610735.610734.533184.533177.533181.533162.533171.533169.533182.533176.533173.533183.533179.533178.533166.&column=measure-533175.&filter=area-518362.&filter=cov_vac_dose-533170."
SECOND_DOSES = "https://sampo.thl.fi/pivot/prod/en/vaccreg/cov19cov/fact_cov19cov.csv?&row=dateweek20201226-525467.525455.525460.525438.525454.525462.525441.527848.531437.533036.533161.533828.533893.537676.539117.547307.547361.547509.547559.566011.567632.568219.580342.580442.581008.581100.590261.600765.601510.601865.601960.601981.602272.602655.610782.610832.617752.618236.619370.627523.628801.629136.629319.630310.639251.639303.642077.642795.650935.651376.651491.&column=cov_vac_age-590256.610735.610734.533184.533177.533181.533162.533171.533169.533182.533176.533173.533183.533179.533178.533166.&column=measure-533175.&filter=area-518362.&filter=cov_vac_dose-533164."


def read_and_process(source: str, metric: str) -> pd.Series:
    return (
        pd.read_csv(source, sep=";", usecols=["Time", "val"])
        .groupby("Time", as_index=False)
        .sum()
        .rename(columns={"val": metric, "Time": "date"})
    )


def week_number_to_date(week_string):
    year, week = re.search(r"Year (\d+) Week (\d+)", week_string).groups()
    date = datetime.datetime.strptime(year + "-" + week + "-0", "%Y-%W-%w")
    if date >= datetime.datetime.now():
        return str(datetime.date.today() - datetime.timedelta(days=1))[:10]
    else:
        return str(date)[:10]


def read() -> pd.Series:

    all_doses = read_and_process(ALL_DOSES, "total_vaccinations")
    first_doses = read_and_process(FIRST_DOSES, "people_vaccinated")
    second_doses = read_and_process(SECOND_DOSES, "people_fully_vaccinated")

    df = all_doses.merge(first_doses, on="date", how="outer").merge(second_doses, on="date", how="outer")

    df["date"] = df.date.apply(week_number_to_date)
    df = df.sort_values("date")

    df["total_vaccinations"] = df.total_vaccinations.cumsum()
    df["people_vaccinated"] = df.people_vaccinated.cumsum()
    df["people_fully_vaccinated"] = df.people_fully_vaccinated.cumsum()

    df = df.assign(
        total_boosters=df.total_vaccinations - df.people_vaccinated - df.people_fully_vaccinated,
        location="Finland",
        source_url="https://sampo.thl.fi/pivot/prod/en/vaccreg/cov19cov",
    ).drop_duplicates(["date"], keep=False)

    return df


def pipeline(ds: pd.Series) -> pd.Series:
    return ds.pipe(
        build_vaccine_timeline,
        {
            "Pfizer/BioNTech": "2020-12-31",
            "Oxford/AstraZeneca": "2021-02-07",
            "Moderna": "2021-02-07",
        },
    )


def main():
    read().pipe(pipeline).to_csv(paths.out_vax("Finland"), index=False)


if __name__ == "__main__":
    main()
