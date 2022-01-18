import os

from cowidev.utils.utils import get_project_dir
from cowidev.megafile.steps.cgrt import get_cgrt
from cowidev.megafile.steps.hosp import get_hosp
from cowidev.megafile.steps.jhu import get_jhu
from cowidev.megafile.steps.reprod import get_reprod
from cowidev.megafile.steps.test import get_testing
from cowidev.megafile.steps.variants import get_variants
from cowidev.megafile.steps.vax import get_vax


INPUT_DIR = os.path.abspath(os.path.join(get_project_dir(), "scripts", "input"))
GRAPHER_DIR = os.path.abspath(os.path.join(get_project_dir(), "scripts", "grapher"))
DATA_DIR = os.path.abspath(os.path.join(get_project_dir(), "public", "data"))


def get_base_dataset():
    """Get owid datasets from: jhu, reproduction rate, hospitalizations, testing ,vaccinations, CGRT."""
    print("Fetching JHU dataset…")
    jhu = get_jhu(jhu_dir=os.path.join(get_project_dir(), "public", "data", "jhu"))

    print("Fetching reproduction rate…")
    reprod = get_reprod(
        file_url="https://github.com/crondonm/TrackingR/raw/main/Estimates-Database/database.csv",
        country_mapping=os.path.join(INPUT_DIR, "reproduction", "reprod_country_standardized.csv"),
    )

    print("Fetching hospital dataset…")
    hosp = get_hosp(data_file=os.path.join(GRAPHER_DIR, "COVID-2019 - Hospital & ICU.csv"))

    print("Fetching testing dataset…")
    testing = get_testing()

    print("Fetching vaccination dataset…")
    vax = get_vax(data_file=os.path.join(DATA_DIR, "vaccinations", "vaccinations.csv"))
    vax = vax[-vax.location.isin(["England", "Northern Ireland", "Scotland", "Wales"])]

    print("Fetching OxCGRT dataset…")
    cgrt = get_cgrt(
        bsg_latest=os.path.join(INPUT_DIR, "bsg", "latest.csv"),
        country_mapping=os.path.join(INPUT_DIR, "bsg", "bsg_country_standardised.csv"),
    )

    print("Fetching variants dataset…")
    variants = get_variants(
        variants_file="s3://covid-19/internal/variants/covid-variants.csv",
        cases_file=os.path.join(DATA_DIR, "jhu", "full_data.csv"),
    )

    # Big merge
    return (
        jhu.merge(reprod, on=["date", "location"], how="outer")
        .merge(hosp, on=["date", "location"], how="outer")
        .merge(testing, on=["date", "location"], how="outer")
        .merge(vax, on=["date", "location"], how="outer")
        .merge(cgrt, on=["date", "location"], how="left")
        .merge(variants, on=["date", "location"], how="left")
        .sort_values(["location", "date"])
    )
