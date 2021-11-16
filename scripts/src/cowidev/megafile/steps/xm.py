import os
import pandas as pd


def add_excess_mortality(df: pd.DataFrame, data_file: str) -> pd.DataFrame:
    column_mapping = {
        "p_proj_all_ages": "excess_mortality",  # excess_mortality_perc_weekly
        "cum_p_proj_all_ages": "excess_mortality_cumulative",  # excess_mortality_perc_cum
        "cum_excess_proj_all_ages": "excess_mortality_cumulative_absolute",  # excess_mortality_count_cum
        "cum_excess_per_million_proj_all_ages": "excess_mortality_cumulative_per_million",  # excess_mortality_count_cum_pm
        "excess_proj_all_ages": "excess_mortality_count_week",  # excess_mortality_count_week
        "excess_per_million_proj_all_ages": "excess_mortality_count_week_pm",  # excess_mortality_count_week_pm
    }
    xm = pd.read_csv(
        data_file,
        usecols=["location", "date"] + list(column_mapping.keys()),
    )
    df = df.merge(xm, how="left", on=["location", "date"]).rename(columns=column_mapping)
    return df
