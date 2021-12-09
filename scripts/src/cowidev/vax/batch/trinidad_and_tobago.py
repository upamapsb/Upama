import pandas as pd

from cowidev.utils import paths
from cowidev.utils.clean import clean_date_series
from cowidev.utils.web import request_json
from cowidev.vax.utils.files import load_query, load_data
from cowidev.vax.utils.utils import build_vaccine_timeline, make_monotonic


def read(source: str) -> pd.DataFrame:
    params = load_query("trinidad-and-tobago-metrics", to_str=False)
    data = request_json(source, params=params)
    return parse_data(data)


def parse_data(data: dict) -> int:
    records = [
        {
            "date": x["attributes"]["report_date_str"],
            "people_vaccinated_2dosevax": x["attributes"]["total_vaccinated"],
            "people_fully_vaccinated": x["attributes"]["total_second_dose"],
            "j_and_j": x["attributes"]["fd_j_and_j"],
            "d1_pfizer": x["attributes"]["fd_pfizer"],
            "d1_sinopharm": x["attributes"]["fd_sinopharm"],
        }
        for x in data["features"]
    ]
    return pd.DataFrame.from_records(records)


def format_date(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(date=clean_date_series(df.date, "%d/%m/%Y")).sort_values("date")


def enrich_vaccine_name(df: pd.DataFrame) -> pd.DataFrame:
    vaccine_timeline = {
        "Oxford/AstraZeneca": "2021-02-15",
        "Johnson&Johnson": df.loc[df.j_and_j.notnull(), "date"].min(),
        "Pfizer/BioNTech": df.loc[df.d1_pfizer.notnull(), "date"].min(),
        "Sinopharm/Beijing": df.loc[df.d1_sinopharm.notnull(), "date"].min(),
    }
    return (
        df.pipe(build_vaccine_timeline, vaccine_timeline)
        .drop(columns=["d1_pfizer", "d1_sinopharm"])
        .dropna(subset=["people_vaccinated_2dosevax"])
    )


def calculate_metrics(df: pd.DataFrame) -> pd.DataFrame:
    df["people_fully_vaccinated"] = df.people_fully_vaccinated.fillna(0)
    df["people_vaccinated"] = df.people_vaccinated_2dosevax.fillna(0) + df.j_and_j.fillna(0)
    df["total_vaccinations"] = df.people_vaccinated + df.people_fully_vaccinated
    return df.drop(columns=["people_vaccinated_2dosevax", "j_and_j"])


def enrich_location(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(location="Trinidad and Tobago")


def enrich_source(df: pd.DataFrame, source: str) -> pd.DataFrame:
    return df.assign(source_url=source)


def merge_legacy(df: pd.DataFrame) -> pd.DataFrame:
    df_legacy = load_data("trinidad-and-tobago-legacy")
    df_legacy = df_legacy[~df_legacy.date.isin(df.date)]
    return pd.concat([df, df_legacy]).sort_values("date")


def pipeline(df: pd.DataFrame, source: str) -> pd.DataFrame:
    return (
        df.pipe(format_date)
        .pipe(enrich_vaccine_name)
        .pipe(calculate_metrics)
        .pipe(enrich_location)
        .pipe(enrich_source, source)
        .pipe(merge_legacy)
        .pipe(make_monotonic)
    )


def main():
    source_ref = "https://experience.arcgis.com/experience/59226cacd2b441c7a939dca13f832112/"
    source = (
        "https://services3.arcgis.com/x3I4DqUw3b3MfTwQ/arcgis/rest/services/service_7a519502598f492a9094fd0ad503cf80/"
        "FeatureServer/0/query"
    )
    destination = paths.out_vax("Trinidad and Tobago")
    read(source).pipe(pipeline, source_ref).to_csv(destination, index=False)


if __name__ == "__main__":
    main()
