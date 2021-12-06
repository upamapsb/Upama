import numpy as np
import pandas as pd


def get_vax(data_file):
    vax = pd.read_csv(
        data_file,
        usecols=[
            "location",
            "date",
            "total_vaccinations",
            "total_vaccinations_per_hundred",
            "daily_vaccinations_raw",
            "daily_vaccinations",
            "daily_vaccinations_per_million",
            "people_vaccinated",
            "people_vaccinated_per_hundred",
            "people_fully_vaccinated",
            "people_fully_vaccinated_per_hundred",
            "total_boosters",
            "total_boosters_per_hundred",
            "daily_people_vaccinated",
            "daily_people_vaccinated_per_hundred",
        ],
    )
    vax = vax.rename(
        columns={
            "daily_vaccinations_raw": "new_vaccinations",
            "daily_vaccinations": "new_vaccinations_smoothed",
            "daily_vaccinations_per_million": "new_vaccinations_smoothed_per_million",
            "daily_people_vaccinated": "new_people_vaccinated_smoothed",
            "daily_people_vaccinated_per_hundred": "new_people_vaccinated_smoothed_per_hundred",
        }
    )
    rounded_cols = [
        "total_vaccinations_per_hundred",
        "people_vaccinated_per_hundred",
        "people_fully_vaccinated_per_hundred",
        "total_boosters_per_hundred",
    ]
    vax[rounded_cols] = vax[rounded_cols].round(3)
    return vax


def _add_rolling(df: pd.DataFrame) -> pd.DataFrame:
    last_known_date = df.loc[df.total_vaccinations.notnull(), "date"].max()
    for n_months in (6, 9, 12):
        n_days = round(365.2425 * n_months / 12)
        df[f"rolling_vaccinations_{n_months}m"] = (
            df.total_vaccinations.interpolate(method="linear").diff().rolling(n_days, min_periods=1).sum().round()
        )
        df.loc[df.date > last_known_date, f"rolling_vaccinations_{n_months}m"] = np.NaN
        df[f"rolling_vaccinations_{n_months}m_per_hundred"] = (
            df[f"rolling_vaccinations_{n_months}m"] * 100 / df.population
        ).round(2)
    return df


def add_rolling_vaccinations(df: pd.DataFrame) -> pd.DataFrame:
    return df.groupby("location").apply(_add_rolling).reset_index(drop=True)
