import pandas as pd


vaccine_mapping = {
    "BioNTechPfizer": "Pfizer/BioNTech",
    "Moderna": "Moderna",
    "AstraZeneca": "Oxford/AstraZeneca",
    "Janssen": "Johnson&Johnson",
}

one_dose_vaccines = ["Janssen"]


def read(source: str) -> pd.DataFrame:
    return pd.read_csv(
        source, sep=";", usecols=["date", "state_name", "vaccine", "dose_number", "doses_administered_cumulative"]
    )


def filter_rows(df: pd.DataFrame) -> pd.DataFrame:
    return df[(df["state_name"] == "Österreich") & (df.vaccine != "Other")].drop(columns="state_name")


def check_vaccine_names(df: pd.DataFrame) -> pd.DataFrame:
    vaccine_names = set(df.vaccine)
    unknown_vaccines = set(vaccine_names).difference(vaccine_mapping.keys())
    if unknown_vaccines:
        raise ValueError("Found unknown vaccines: {}".format(unknown_vaccines))
    return df


def format_date(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(date=df.date.str.slice(0, 10))


def reshape(df: pd.DataFrame) -> pd.DataFrame:
    return df.pivot(
        index=["date", "vaccine"], columns="dose_number", values="doses_administered_cumulative"
    ).reset_index()


def calculate_metrics(df: pd.DataFrame) -> pd.DataFrame:
    assert [*df.columns] == ["date", "vaccine", 1, 2, 3], "Wrong list of columns! Maybe a 4th dose was added?"

    # Total vaccinations
    df.loc[:, "total_vaccinations"] = df[1] + df[2] + df[3]

    # People vaccinated
    df.loc[:, "people_vaccinated"] = df[1]

    # People fully vaccinated
    df.loc[df.vaccine.isin(one_dose_vaccines), "people_fully_vaccinated"] = df[1]
    df.loc[-df.vaccine.isin(one_dose_vaccines), "people_fully_vaccinated"] = df[2]

    # Total boosters
    df.loc[df.vaccine.isin(one_dose_vaccines), "total_boosters"] = df[2] + df[3]
    df.loc[-df.vaccine.isin(one_dose_vaccines), "total_boosters"] = df[3]

    return (
        df[
            [
                "date",
                "people_vaccinated",
                "people_fully_vaccinated",
                "total_vaccinations",
                "total_boosters",
            ]
        ]
        .groupby("date", as_index=False)
        .sum()
    )


def enrich_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(
        location="Austria",
        source_url="https://info.gesundheitsministerium.gv.at/opendata/",
    )


def enrich_vaccines(df: pd.DataFrame) -> pd.DataFrame:
    def _make_list(date: str) -> str:
        vax_list = ["Pfizer/BioNTech"]
        if date >= "2021-01-15":
            vax_list.append("Moderna")
        if date >= "2021-02-08":
            vax_list.append("Oxford/AstraZeneca")
        if date >= "2021-03-15":
            vax_list.append("Johnson&Johnson")
        return ", ".join(sorted(vax_list))

    df["vaccine"] = df.date.apply(_make_list)
    return df


def pipeline(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.pipe(filter_rows)
        .pipe(check_vaccine_names)
        .pipe(format_date)
        .pipe(reshape)
        .pipe(calculate_metrics)
        .pipe(enrich_columns)
        .pipe(enrich_vaccines)
        .sort_values("date")
    )


def main(paths):
    source = "https://info.gesundheitsministerium.gv.at/data/COVID19_vaccination_doses_timeline.csv"
    destination = paths.tmp_vax_out("Austria")
    read(source).pipe(pipeline).to_csv(destination, index=False)
