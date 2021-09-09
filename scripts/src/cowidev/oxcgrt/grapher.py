from datetime import datetime

import pandas as pd
from cowidev.grapher.db.base import GrapherBaseUpdater
from cowidev.utils.utils import time_str_grapher, get_filename
from cowidev.utils.clean.dates import DATE_FORMAT

ZERO_DAY = "2020-01-01"
zero_day = datetime.strptime(ZERO_DAY, DATE_FORMAT)

URL_VACCINE = "https://raw.githubusercontent.com/OxCGRT/covid-policy-tracker/master/data/OxCGRT_vaccines_full.csv"


def run_grapheriser(input_path: str, input_path_country_std: str, output_path: str):
    cgrt = pd.read_csv(
        input_path,
        low_memory=False,
        usecols=[
            "CountryName",
            "Date",
            "RegionCode",
            "C1_School closing",
            "C2_Workplace closing",
            "C3_Cancel public events",
            "C4_Restrictions on gatherings",
            "C5_Close public transport",
            "C6_Stay at home requirements",
            "C7_Restrictions on internal movement",
            "C8_International travel controls",
            "E1_Income support",
            "E2_Debt/contract relief",
            "E3_Fiscal measures",
            "E4_International support",
            "H1_Public information campaigns",
            "H2_Testing policy",
            "H3_Contact tracing",
            "H4_Emergency investment in healthcare",
            "H5_Investment in vaccines",
            "H6_Facial Coverings",
            "H7_Vaccination policy",
            "StringencyIndex",
            "ContainmentHealthIndex",
        ],
    )
    country_mapping = pd.read_csv(input_path_country_std)

    cgrt = cgrt[cgrt.RegionCode.isnull()].drop(columns="RegionCode")

    vax = pd.read_csv(
        URL_VACCINE,
        low_memory=False,
        usecols=["CountryName", "Date", "V2_Vaccine Availability (summary)"],
    )
    cgrt = pd.merge(cgrt, vax, how="outer", on=["CountryName", "Date"], validate="one_to_one")

    cgrt.loc[:, "Date"] = pd.to_datetime(cgrt["Date"], format="%Y%m%d").map(lambda date: (date - zero_day).days)
    cgrt = country_mapping.merge(cgrt, on="CountryName", how="right")

    missing_from_mapping = cgrt[cgrt["Country"].isna()]["CountryName"].unique()
    if len(missing_from_mapping) > 0:
        raise Exception(f"Missing countries in mapping: {missing_from_mapping}")

    cgrt = cgrt.drop(columns=["CountryName"])

    rename_dict = {
        "Date": "Year",
        "C1_School closing": "school_closures",
        "C2_Workplace closing": "workplace_closures",
        "C3_Cancel public events": "cancel_public_events",
        "C5_Close public transport": "close_public_transport",
        "H1_Public information campaigns": "public_information_campaigns",
        "C7_Restrictions on internal movement": "restrictions_internal_movements",
        "C8_International travel controls": "international_travel_controls",
        "E3_Fiscal measures": "fiscal_measures",
        "H4_Emergency investment in healthcare": "emergency_investment_healthcare",
        "H5_Investment in vaccines": "investment_vaccines",
        "H3_Contact tracing": "contact_tracing",
        "H6_Facial Coverings": "facial_coverings",
        "StringencyIndex": "stringency_index",
        "ContainmentHealthIndex": "containment_index",
        "C4_Restrictions on gatherings": "restriction_gatherings",
        "C6_Stay at home requirements": "stay_home_requirements",
        "E1_Income support": "income_support",
        "E2_Debt/contract relief": "debt_relief",
        "E4_International support": "international_support",
        "H7_Vaccination policy": "vaccination_policy",
        "H2_Testing policy": "testing_policy",
        "V2_Vaccine Availability (summary)": "vaccine_eligibility",
    }

    cgrt = cgrt.rename(columns=rename_dict).sort_values(["Country", "Year"])
    cgrt.to_csv(output_path, index=False)


def run_db_updater(input_path: str):
    dataset_name = get_filename(input_path)
    GrapherBaseUpdater(
        dataset_name=dataset_name,
        source_name=(
            "Oxford COVID-19 Government Response Tracker, Blavatnik School of Government, University of Oxford"
            f" – Last updated {time_str_grapher()} (London time)"
        ),
        zero_day=ZERO_DAY,
        slack_notifications=True,
    ).run()
