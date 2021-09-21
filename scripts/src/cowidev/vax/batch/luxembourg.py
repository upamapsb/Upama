import pandas as pd

from cowidev.vax.utils.utils import make_monotonic


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
    Since 2021-04-14 Luxembourg is using J&J, therefore dose2 == people_fully_vaccinated no longer
    works. As a temporary fix while they report the necessary data, we're inserting one PDF report
    in late September 2021 to avoid showing an old value for people_fully_vaccinated in dashboard
    that re-use our latest totals without showing how old they are.
    The publisher was contacted on 2021-O9-21 https://twitter.com/redouad/status/1439992459166158857
    """
    df.loc[df.date >= "2021-04-14", "people_fully_vaccinated"] = pd.NA
    fix = pd.DataFrame(
        {
            "date": [pd.to_datetime("2021-09-16")],
            "people_vaccinated": 414505,
            "people_fully_vaccinated": 399522,
            "total_boosters": pd.NA,
            "total_vaccinations": 777109,
            "source_url": [
                "https://data.public.lu/en/datasets/donnees-covid19/#resource-a3c13d63-6e1d-4bd6-9ba4-2dba5cf9ad5b"
            ],
        }
    )
    df = pd.concat([df, fix]).drop_duplicates(subset="date", keep="last")
    df = make_monotonic(df)
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
        .pipe(calculate_running_totals)
        .pipe(enrich_source, source)
        .pipe(correct_time_series)
        .pipe(enrich_location)
        .pipe(enrich_vaccines)
    )


def main(paths):
    source_file = "https://data.public.lu/en/datasets/r/a3c13d63-6e1d-4bd6-9ba4-2dba5cf9ad5b"
    source_page = "https://data.public.lu/en/datasets/donnees-covid19/#_"
    destination = paths.tmp_vax_out("Luxembourg")
    read(source_file).pipe(pipeline, source_page).to_csv(destination, index=False)


if __name__ == "__main__":
    main()
