import os
from glob import glob

import pandas as pd


def get_latest_file(path, extension):
    files = glob(os.path.join(path, f"*.{extension}"))
    return max(files, key=os.path.getctime)


def make_monotonic(df: pd.DataFrame, max_removed_rows=10) -> pd.DataFrame:
    # Forces vaccination time series to become monotonic.
    # The algorithm assumes that the most recent values are the correct ones,
    # and therefore removes previous higher values.
    n_rows_before = len(df)

    df = df.sort_values("date")
    metrics = ("total_vaccinations", "people_vaccinated", "people_fully_vaccinated")
    for metric in metrics:
        while not df[metric].ffill().fillna(0).is_monotonic:
            diff = df[metric].ffill().shift(-1) - df[metric].ffill()
            df = df[(diff >= 0) | (diff.isna())]

    if max_removed_rows is not None:
        if n_rows_before - len(df) > max_removed_rows:
            raise Exception(f"More than {max_removed_rows} rows removed by make_monotonic() - check the data.")

    return df
