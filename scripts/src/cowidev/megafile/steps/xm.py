import os
import pandas as pd


def add_excess_mortality(df: pd.DataFrame, wmd_hmd_file: str, economist_file: str) -> pd.DataFrame:

    # XM data from HMD & WMD
    column_mapping = {
        "p_proj_all_ages": "excess_mortality",  # excess_mortality_perc_weekly
        "cum_p_proj_all_ages": "excess_mortality_cumulative",  # excess_mortality_perc_cum
        "cum_excess_proj_all_ages": "excess_mortality_cumulative_absolute",  # excess_mortality_count_cum
        "cum_excess_per_million_proj_all_ages": "excess_mortality_cumulative_per_million",  # excess_mortality_count_cum_pm
        "excess_proj_all_ages": "excess_mortality_count_week",  # excess_mortality_count_week
        "excess_per_million_proj_all_ages": "excess_mortality_count_week_pm",  # excess_mortality_count_week_pm
    }
    wmd_hmd = pd.read_csv(wmd_hmd_file, usecols=["location", "date"] + list(column_mapping.keys()))
    df = df.merge(wmd_hmd, how="left", on=["location", "date"]).rename(columns=column_mapping)

    # XM data from The Economist
    econ = pd.read_csv(
        economist_file,
        usecols=[
            "country",
            "date",
            "cumulative_estimated_daily_excess_deaths",
            "cumulative_estimated_daily_excess_deaths_ci_95_top",
            "cumulative_estimated_daily_excess_deaths_ci_95_bot",
            "cumulative_estimated_daily_excess_deaths_per_100k",
            "cumulative_estimated_daily_excess_deaths_ci_95_top_per_100k",
            "cumulative_estimated_daily_excess_deaths_ci_95_bot_per_100k",
        ],
    ).rename(columns={"country": "location"})
    df = df.merge(econ, how="left", on=["location", "date"])

    return df
