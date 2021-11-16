"merge"
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
