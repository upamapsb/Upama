import pandas as pd

from cowidev.grapher.files import Grapheriser, Exploriser


NUM_SEQUENCES_TOTAL_THRESHOLD = 30


def filter_by_num_sequences(df: pd.DataFrame) -> pd.DataFrame:
    msk = df.num_sequences_total < NUM_SEQUENCES_TOTAL_THRESHOLD
    # Info
    _sk_perc_rows = round(100 * (msk.sum() / len(df)), 2)
    _sk_num_countries = df.loc[msk, "location"].nunique()
    _sk_countries_top = df[msk]["location"].value_counts().head(10).to_dict()
    print(
        f"Skipping {msk.sum()} datapoints ({_sk_perc_rows}%), affecting {_sk_num_countries} countries. Some are:"
        f" {_sk_countries_top}"
    )
    return df[~msk]


def run_grapheriser(input_path: str, output_path: str, input_path_seq: str, output_path_seq: str):
    # Variants
    Grapheriser(
        pivot_column="variant",
        pivot_values=["num_sequences", "perc_sequences"],
        fillna_0=True,
        function_input=filter_by_num_sequences,
        suffixes=["", "_percentage"],
    ).run(input_path, output_path)
    # Sequencing
    Grapheriser(
        fillna_0=True,
    ).run(input_path_seq, output_path_seq)


def run_explorerizer(input_path: str, output_path: str):
    Exploriser(
        pivot_column="variant",
        pivot_values="perc_sequences",
        function_input=filter_by_num_sequences,
    ).run(input_path, output_path)


def run_db_updater(input_path: str):
    raise NotImplementedError("Not yet implemented")
