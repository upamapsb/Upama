import os
import pandas as pd

from cowidev.utils.utils import get_project_dir


INPUT_DIR = os.path.join(get_project_dir(), "scripts", "input")
GRAPHER_DIR = os.path.join(get_project_dir(), "scripts", "grapher")
DATA_DIR = os.path.join(get_project_dir(), "public", "data")
VACCINATIONS_CSV = os.path.join(DATA_DIR, "vaccinations", "vaccinations.csv")
TESTING_CSV = os.path.join(DATA_DIR, "testing", "covid-testing-all-observations.csv")
CASES_CSV = os.path.join(DATA_DIR, "jhu", "total_cases.csv")
DEATHS_CSV = os.path.join(DATA_DIR, "jhu", "total_deaths.csv")
HOSP_CSV = os.path.join(GRAPHER_DIR, "COVID-2019 - Hospital & ICU.csv")
REPR_CSV = "https://github.com/crondonm/TrackingR/raw/main/Estimates-Database/database.csv"
POL_CSV = os.path.join(INPUT_DIR, "bsg", "latest.csv")
CODEBOOK_CSV = os.path.join(DATA_DIR, "owid-covid-codebook.csv")


def get_excluded_locations():
    df = pd.read_csv(VACCINATIONS_CSV)
    codes = [code for code in df["iso_code"].unique() if "OWID_" in code]
    EXCLUDE_LOCATIONS = set(
        df[df.iso_code.isin(codes)].location.unique().tolist() + ["2020 Summer Olympics athletes & staff"]
    )
    EXCLUDE_LOCATIONS.remove("Kosovo")
    EXCLUDE_ISOS = df[df.location.isin(EXCLUDE_LOCATIONS)].iso_code.unique()
    return EXCLUDE_LOCATIONS, EXCLUDE_ISOS


EXCLUDE_LOCATIONS, EXCLUDE_ISOS = get_excluded_locations()


def get_num_countries_by_iso(iso_code_colname, csv_filepath=None, df=None):
    if df is None:
        df = pd.read_csv(csv_filepath, low_memory=False)
    codes = [code for code in df[iso_code_colname].dropna().unique() if code not in EXCLUDE_ISOS]
    return len(codes)


def get_num_countries_by_location(csv_filepath, location_colname, low_memory=True):
    df = pd.read_csv(csv_filepath, low_memory=low_memory)
    locations = [loc for loc in df[location_colname].dropna().unique() if loc not in EXCLUDE_LOCATIONS]
    return len(locations)


def get_num_countries_jhu(csv_filepath):
    df = pd.read_csv(csv_filepath, low_memory=False)
    columns = df.columns
    return len(columns[~columns.isin(EXCLUDE_LOCATIONS)]) - 1


def load_macro_df():
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
    dfs = []
    for var, file in macro_variables.items():
        dfs.append(pd.read_csv(os.path.join(INPUT_DIR, file), usecols=["iso_code", var]))
    df = pd.concat(dfs)
    return df


def get_variable_section():
    template = """### {title}\n{table}"""
    df = pd.read_csv(CODEBOOK_CSV).rename(columns={"description": "Description"})
    df = df.assign(Variable=df.column.apply(lambda x: f"`{x}`"))
    variable_description = []
    categories = list(filter(lambda x: x != "Others", sorted(df.category.unique()))) + ["Others"]
    for cat in categories:
        df_ = df[df.category == cat]
        table = df_[["Variable", "Description"]].to_markdown(index=False)
        variable_description.append(template.format(title=cat, table=table))
    return variable_description


def get_placeholder():
    placeholders = {
        "num_countries_vaccinations": get_num_countries_by_iso(
            csv_filepath=VACCINATIONS_CSV, iso_code_colname="iso_code"
        ),
        "num_countries_testing": get_num_countries_by_iso(csv_filepath=TESTING_CSV, iso_code_colname="ISO code"),
        "num_countries_cases": get_num_countries_jhu(csv_filepath=CASES_CSV),
        "num_countries_deaths": get_num_countries_jhu(csv_filepath=DEATHS_CSV),
        "num_countries_hospital": get_num_countries_by_location(csv_filepath=HOSP_CSV, location_colname="Country"),
        "num_countries_reproduction": get_num_countries_by_location(
            csv_filepath=REPR_CSV, location_colname="Country/Region"
        ),
        "num_countries_policy": get_num_countries_by_location(
            csv_filepath=POL_CSV,
            location_colname="CountryName",
            low_memory=False,
        ),
        "num_countries_others": get_num_countries_by_iso(df=load_macro_df(), iso_code_colname="iso_code"),
        "variable_description": "\n".join(get_variable_section()),
    }
    return placeholders


def generate_readme(readme_template: str, readme_output: str):
    placeholders = get_placeholder()
    with open(readme_template, "r", encoding="utf-8") as fr:
        s = fr.read()
        s = s.format(**placeholders)
        with open(readme_output, "w", encoding="utf-8") as fw:
            fw.write(s)
