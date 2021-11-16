"merge"
import os
from functools import reduce
import pandas as pd


def get_jhu(jhu_dir: str):
    """
    Reads each COVID-19 JHU dataset located in /public/data/jhu/
    Melts the dataframe to vertical format (1 row per country and date)
    Merges all JHU dataframes into one with outer joins

    Returns:
        jhu {dataframe}
    """

    jhu_variables = [
        "total_cases",
        "new_cases",
        "weekly_cases",
        "total_deaths",
        "new_deaths",
        "weekly_deaths",
        "total_cases_per_million",
        "new_cases_per_million",
        "weekly_cases_per_million",
        "total_deaths_per_million",
        "new_deaths_per_million",
        "weekly_deaths_per_million",
    ]

    data_frames = []

    # Process each file and melt it to vertical format
    for jhu_var in jhu_variables:
        tmp = pd.read_csv(os.path.join(jhu_dir, f"{jhu_var}.csv"))
        country_cols = list(tmp.columns)
        country_cols.remove("date")

        # Carrying last observation forward for International totals to avoid discrepancies
        if jhu_var[:5] == "total":
            tmp = tmp.sort_values("date")
            tmp["International"] = tmp["International"].ffill()

        tmp = (
            pd.melt(tmp, id_vars="date", value_vars=country_cols)
            .rename(columns={"value": jhu_var, "variable": "location"})
            .dropna()
        )

        if jhu_var[:7] == "weekly_":
            tmp[jhu_var] = tmp[jhu_var].div(7).round(3)
            tmp = tmp.rename(
                errors="ignore",
                columns={
                    "weekly_cases": "new_cases_smoothed",
                    "weekly_deaths": "new_deaths_smoothed",
                    "weekly_cases_per_million": "new_cases_smoothed_per_million",
                    "weekly_deaths_per_million": "new_deaths_smoothed_per_million",
                },
            )
        else:
            tmp[jhu_var] = tmp[jhu_var].round(3)
        data_frames.append(tmp)

    # Outer join between all files
    jhu = reduce(
        lambda left, right: pd.merge(left, right, on=["date", "location"], how="outer"),
        data_frames,
    )

    return jhu
