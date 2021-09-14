import pandas as pd


def read(source: str) -> pd.DataFrame:
    return pd.read_excel(source)


def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(
        columns={
            "Date": "date",
            "Nombre de dose 1": "people_vaccinated",
            "Nombre de dose 2": "people_fully_vaccinated",
            "Nombre de dose 3": "total_boosters",
            "Nombre total de doses": "total_vaccinations",
        }
    )


def correct_time_series(df: pd.DataFrame) -> pd.DataFrame:
    """
    Since 2021-04-14 Luxembourg is using J&J, therefore dose2 == people_fully_vaccinated no longer works
    """
    df.loc[df.date >= "2021-04-14", "people_fully_vaccinated"] = pd.NA
    return df


def calculate_running_totals(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values("date")
    df[["people_vaccinated", "people_fully_vaccinated", "total_boosters", "total_vaccinations"]] = df[
        ["people_vaccinated", "people_fully_vaccinated", "total_boosters", "total_vaccinations"]
    ].cumsum()
    return df


def enrich_location(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(location="Luxembourg")


def enrich_vaccines(df: pd.DataFrame) -> pd.DataFrame:
    df = df.assign(vaccine="Pfizer/BioNTech")
    df.loc[df.date >= "2021-01-20", "vaccine"] = "Moderna, Pfizer/BioNTech"
    df.loc[df.date >= "2021-02-10", "vaccine"] = "Moderna, Oxford/AstraZeneca, Pfizer/BioNTech"
    df.loc[df.date >= "2021-04-14", "vaccine"] = "Johnson&Johnson, Moderna, Oxford/AstraZeneca, Pfizer/BioNTech"
    return df


def enrich_source(df: pd.DataFrame, source: str) -> pd.DataFrame:
    return df.assign(source_url=source)


def pipeline(df: pd.DataFrame, source: str) -> pd.DataFrame:
    return (
        df.pipe(rename_columns)
        .pipe(correct_time_series)
        .pipe(calculate_running_totals)
        .pipe(enrich_location)
        .pipe(enrich_vaccines)
        .pipe(enrich_source, source)
    )


def main(paths):
    source_file = "https://data.public.lu/en/datasets/r/a3c13d63-6e1d-4bd6-9ba4-2dba5cf9ad5b"
    source_page = "https://data.public.lu/en/datasets/donnees-covid19/#_"
    destination = paths.tmp_vax_out("Luxembourg")
    read(source_file).pipe(pipeline, source_page).to_csv(destination, index=False)


if __name__ == "__main__":
    main()
