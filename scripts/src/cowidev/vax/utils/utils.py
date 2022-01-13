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
    dates_before = set(df.date)
    df_before = df.copy()

    df = df.sort_values("date")
    metrics = ("total_vaccinations", "people_vaccinated", "people_fully_vaccinated")
    for metric in metrics:
        while not df[metric].ffill().fillna(0).is_monotonic:
            diff = df[metric].ffill().shift(-1) - df[metric].ffill()
            df = df[(diff >= 0) | (diff.isna())]
    dates_now = set(df.date)

    if max_removed_rows is not None:
        num_removed_rows = n_rows_before - len(df)
        if num_removed_rows > max_removed_rows:
            dates_wrong = dates_before.difference(dates_now)
            df_wrong = df_before[df_before.date.isin(dates_wrong)]
            raise Exception(
                f"{num_removed_rows} rows have been removed. That is more than maximum allowed ({max_removed_rows}) by"
                f" make_monotonic() - check the data. Check \n{df_wrong}"  # {', '.join(sorted(dates_wrong))}"
            )

    return df


def build_vaccine_timeline(df: pd.DataFrame, vaccine_timeline: dict) -> pd.DataFrame:
    """
    vaccine_timeline: dictionary of "vaccine" -> "start_date"
    Example: {
        "Pfizer/BioNTech": "2021-02-24",
        "Sinovac": "2021-03-03",
        "Oxford/AstraZeneca": "2021-05-03",
        "CanSino": "2021-05-09",
        "Sinopharm": "2021-09-18",
    }
    """

    def _build_vaccine_row(date, vaccine_timeline: dict):
        vaccines = [k for k, v in vaccine_timeline.items() if v <= date]
        return ", ".join(sorted(list(set(vaccines))))

    df["vaccine"] = df.date.apply(_build_vaccine_row, vaccine_timeline=vaccine_timeline)
    return df
