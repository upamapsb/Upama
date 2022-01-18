import os
from datetime import date

import pandas as pd

from cowidev.utils.utils import get_project_dir, export_timestamp
from cowidev.megafile.steps import (
    get_base_dataset,
    add_macro_variables,
    add_excess_mortality,
    add_rolling_vaccinations,
)
from cowidev.megafile.export import (
    create_internal,
    create_dataset,
    create_latest,
    generate_readme,
)


INPUT_DIR = os.path.abspath(os.path.join(get_project_dir(), "scripts", "input"))
DATA_DIR = os.path.abspath(os.path.join(get_project_dir(), "public", "data"))
DATA_VAX_COUNTRIES_DIR = os.path.abspath(os.path.join(DATA_DIR, "vaccinations", "country_data"))
ANNOTATIONS_PATH = os.path.abspath(os.path.join(get_project_dir(), "scripts", "scripts", "annotations_internal.yaml"))
README_TMP = os.path.join(get_project_dir(), "scripts", "scripts", "README.md.template")
README_FILE = os.path.join(DATA_DIR, "README.md")


def generate_megafile():
    """Generate megafile data."""
    all_covid = get_base_dataset()

    # Remove today's datapoint
    all_covid = all_covid[all_covid["date"] < str(date.today())]

    # Exclude some entities from megafile
    excluded = ["Summer Olympics 2020"]
    all_covid = all_covid[-all_covid.location.isin(excluded)]

    # Add ISO codes
    print("Adding ISO codes…")
    iso_codes = pd.read_csv(os.path.join(INPUT_DIR, "iso", "iso3166_1_alpha_3_codes.csv"))

    missing_iso = set(all_covid.location).difference(set(iso_codes.location))
    if len(missing_iso) > 0:
        print(missing_iso)
        raise Exception("Missing ISO code for some locations")

    all_covid = iso_codes.merge(all_covid, on="location")

    # Add continents
    print("Adding continents…")
    continents = pd.read_csv(
        os.path.join(INPUT_DIR, "owid", "continents.csv"),
        names=["_1", "iso_code", "_2", "continent"],
        usecols=["iso_code", "continent"],
        header=0,
    )

    all_covid = continents.merge(all_covid, on="iso_code", how="right")

    # Add macro variables
    # - the key is the name of the variable of interest
    # - the value is the path to the corresponding file
    macro_variables = {
        "population": "un/population_latest.csv",
        "population_density": "wb/population_density.csv",
        "median_age": "un/median_age.csv",
        "aged_65_older": "wb/aged_65_older.csv",
        "aged_70_older": "un/aged_70_older.csv",
        "gdp_per_capita": "wb/gdp_per_capita.csv",
        "extreme_poverty": "wb/extreme_poverty.csv",
        "cardiovasc_death_rate": "gbd/cardiovasc_death_rate.csv",
        "diabetes_prevalence": "wb/diabetes_prevalence.csv",
        "female_smokers": "wb/female_smokers.csv",
        "male_smokers": "wb/male_smokers.csv",
        "handwashing_facilities": "un/handwashing_facilities.csv",
        "hospital_beds_per_thousand": "owid/hospital_beds.csv",
        "life_expectancy": "owid/life_expectancy.csv",
        "human_development_index": "un/human_development_index.csv",
    }
    all_covid = add_macro_variables(all_covid, macro_variables, INPUT_DIR)

    # Add excess mortality
    all_covid = add_excess_mortality(
        df=all_covid,
        wmd_hmd_file=os.path.join(DATA_DIR, "excess_mortality", "excess_mortality.csv"),
        economist_file=os.path.join(DATA_DIR, "excess_mortality", "excess_mortality_economist_estimates.csv"),
    )

    # Calculate rolling vaccinations
    all_covid = add_rolling_vaccinations(all_covid)

    # Sort by location and date
    all_covid = all_covid.sort_values(["location", "date"])

    # Check that we only have 1 unique row for each location/date pair
    assert all_covid.drop_duplicates(subset=["location", "date"]).shape == all_covid.shape

    print("Creating internal files…")
    create_internal(
        df=all_covid,
        output_dir=os.path.join(DATA_DIR, "internal"),
        annotations_path=ANNOTATIONS_PATH,
        country_data=DATA_VAX_COUNTRIES_DIR,
    )

    # Drop columns not included in final dataset
    cols_drop = [
        "excess_mortality_count_week",
        "excess_mortality_count_week_pm",
        "share_cases_sequenced",
        "rolling_vaccinations_6m",
        "rolling_vaccinations_6m_per_hundred",
        "rolling_vaccinations_9m",
        "rolling_vaccinations_9m_per_hundred",
        "rolling_vaccinations_12m",
        "rolling_vaccinations_12m_per_hundred",
        "cumulative_estimated_daily_excess_deaths",
        "cumulative_estimated_daily_excess_deaths_ci_95_top",
        "cumulative_estimated_daily_excess_deaths_ci_95_bot",
        "cumulative_estimated_daily_excess_deaths_per_100k",
        "cumulative_estimated_daily_excess_deaths_ci_95_top_per_100k",
        "cumulative_estimated_daily_excess_deaths_ci_95_bot_per_100k",
    ]
    all_covid = all_covid.drop(columns=cols_drop)

    # # Create light versions of complete dataset with only the latest data point
    print("Writing latest…")
    create_latest(all_covid)

    # Create datasets
    create_dataset(all_covid, macro_variables)

    # Store the last updated time
    dir = os.path.join(get_project_dir(), "public", "data")
    export_timestamp("owid-covid-data-last-updated-timestamp.txt", force_directory=dir)  # @deprecate

    print("Generating public/data/README.md")
    generate_readme(readme_template=README_TMP, readme_output=README_FILE)

    # Export timestamp
    export_timestamp("owid-covid-data-last-updated-timestamp-root.txt")

    print("All done!")
