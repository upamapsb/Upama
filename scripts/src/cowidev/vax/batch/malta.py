import pandas as pd

from cowidev.vax.utils.dates import clean_date_series


def read(source: str) -> pd.DataFrame:
    return pd.read_csv(source)


def check_columns(df: pd.DataFrame, expected) -> pd.DataFrame:
    n_columns = df.shape[1]
    if n_columns != expected:
        raise ValueError(
            "The provided input does not have {} columns. It has {} columns".format(
                expected, n_columns
            )
        )
    return df


def rename_columns(df: pd.DataFrame, columns: dict) -> pd.DataFrame:
    return df.rename(columns=columns)


def correct_data(df: pd.DataFrame) -> pd.DataFrame:
    df.loc[
        (df.people_fully_vaccinated == 0) | df.people_fully_vaccinated.isnull(),
        "people_vaccinated",
    ] = df.total_vaccinations
    return df


def format_date(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(date=clean_date_series(df.date, "%d/%m/%Y"))


def enrich_vaccine_name(df: pd.DataFrame) -> pd.DataFrame:
    def _enrich_vaccine_name(date: str) -> str:
        # See timeline in:
        if date < "2021-02-03":
            return "Pfizer/BioNTech"
        if "2021-02-03" <= date < "2021-02-10":
            return "Moderna, Pfizer/BioNTech"
        elif "2021-02-10" <= date < "2021-05-06":
            return "Moderna, Oxford/AstraZeneca, Pfizer/BioNTech"
        elif "2021-05-06" <= date:
            return "Johnson&Johnson, Moderna, Oxford/AstraZeneca, Pfizer/BioNTech"

    return df.assign(vaccine=df.date.apply(_enrich_vaccine_name))


def enrich_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(
        location="Malta",
        source_url="https://github.com/COVID19-Malta/COVID19-Cases",
    )


def exclude_data_points(df: pd.DataFrame) -> pd.DataFrame:
    # The data contains an error that creates a negative change in the people_vaccinated series
    df = df[df.date.astype(str) != "2021-01-24"]

    return df


def pipeline(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.pipe(check_columns, expected=4)
        .pipe(
            rename_columns,
            columns={
                "Date": "date",
                "Total Vaccination Doses": "total_vaccinations",
                "Fully vaccinated (2 of 2 or 1 of 1)": "people_fully_vaccinated",
                "Received one dose": "people_vaccinated",
            },
        )
        .pipe(correct_data)
        .pipe(format_date)
        .pipe(enrich_columns)
        .pipe(enrich_vaccine_name)
        .pipe(exclude_data_points)
    )


def main(paths):
    source = "https://github.com/COVID19-Malta/COVID19-Cases/raw/master/COVID-19%20Malta%20-%20Vaccination%20Data.csv"
    destination = paths.tmp_vax_out("Malta")
    read(source).pipe(pipeline).to_csv(destination, index=False)


if __name__ == "__main__":
    main()
