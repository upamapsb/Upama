import datetime

import pandas as pd


def get_variants(cases_file: str, variants_file: str) -> pd.DataFrame:
    """
    Fetches the processed data from CoVariants.org and merges it with biweekly cases from JHU.
    """
    variants = pd.read_csv(variants_file, usecols=["location", "date", "num_sequences_total"]).drop_duplicates()
    cases = pd.read_csv(cases_file, usecols=["location", "date", "biweekly_cases"]).dropna()

    df = pd.merge(variants, cases, how="inner", on=["location", "date"], validate="one_to_one")

    df["share_cases_sequenced"] = df.num_sequences_total.mul(100).div(df.biweekly_cases).round(1)

    # Data quality: ensure that share is between 0 and 100, and add a 15 day cutoff to avoid showing
    # low sequencing rates due to lags in reporting.
    df = df[(df.share_cases_sequenced <= 100) & (df.share_cases_sequenced >= 0)]
    df = df[df.date <= str(datetime.date.today() - datetime.timedelta(days=15))]

    df = df.drop(columns=["num_sequences_total", "biweekly_cases"])
    return df
