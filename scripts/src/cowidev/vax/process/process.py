from datetime import datetime
import pandas as pd

from cowidev.vax.utils.checks import country_df_sanity_checks
from cowidev.utils.clean import clean_urls, clean_date


def process_location(df: pd.DataFrame, monotonic_check_skip: list = [], anomaly_check_skip: list = []) -> pd.DataFrame:
    # print(df.tail(1))
    # Only report up to previous day to avoid partial reporting
    df = df.assign(date=pd.to_datetime(df.date, dayfirst=True))
    df = df[df.date.dt.date < datetime.now().date()]
    # Default columns for second doses
    if "people_vaccinated" not in df:
        df = df.assign(people_vaccinated=pd.NA)
        df.people_vaccinated = df.people_vaccinated.astype("Int64")
    if "people_fully_vaccinated" not in df:
        df = df.assign(people_fully_vaccinated=pd.NA)
        df.people_fully_vaccinated = df.people_fully_vaccinated.astype("Int64")
    if "total_boosters" not in df:
        df = df.assign(total_boosters=pd.NA)
        df.total_boosters = df.total_boosters.astype("Int64")
    # Avoid decimals
    cols = [
        "total_vaccinations",
        "people_vaccinated",
        "people_partly_vaccinated",
        "people_fully_vaccinated",
        "total_boosters",
    ]
    cols = df.columns.intersection(cols).tolist()
    df[cols] = df[cols].astype(float).astype("Int64").fillna(pd.NA)
    # Order columns and rows
    usecols = [
        "location",
        "date",
        "vaccine",
        "source_url",
    ] + cols
    usecols = df.columns.intersection(usecols).tolist()
    df = df[usecols]
    df = df.sort_values(by="date")
    # Sanity checks
    country_df_sanity_checks(
        df,
        monotonic_check_skip=monotonic_check_skip,
        anomaly_check_skip=anomaly_check_skip,
    )
    # Strip
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    # Date format
    df = df.assign(date=df.date.apply(clean_date))
    # Clean URLs
    df = clean_urls(df)
    return df
