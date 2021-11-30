import pandas as pd


def get_variants(cases_file: str, variants_file: str) -> pd.DataFrame:
    variants = pd.read_csv(variants_file, usecols=["location", "date", "num_sequences_total"]).drop_duplicates()
    cases = pd.read_csv(cases_file, usecols=["location", "date", "biweekly_cases"]).dropna()
    df = pd.merge(variants, cases, how="inner", on=["location", "date"], validate="one_to_one")
    df["share_cases_sequenced"] = 1000 * df.num_sequences_total / df.biweekly_cases
    df = df.drop(columns=["num_sequences_total", "biweekly_cases"])
    return df
