import pandas as pd


def make_monotonic(df: pd.DataFrame) -> pd.DataFrame:
    # Forces time series to become monotonic.
    # The algorithm assumes that the most recent values are the correct ones,
    # and therefore removes previous higher values.
    df = df.sort_values("Date")
    metrics = ("Cumulative total",)
    for metric in metrics:
        while not df[metric].ffill().fillna(0).is_monotonic:
            diff = df[metric].ffill().shift(-1) - df[metric].ffill()
            df = df[(diff >= 0) | (diff.isna())]
    return df
