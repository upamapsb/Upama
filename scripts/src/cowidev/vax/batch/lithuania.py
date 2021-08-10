import json

import requests
import pandas as pd

from cowidev.vax.utils.files import export_metadata


def main(paths):

    DATA_URL = (
        "https://services3.arcgis.com/MF53hRPmwfLccHCj/arcgis/rest/services/"
        "covid_vaccinations_by_drug_name_new/FeatureServer/0/query"
    )
    PARAMS = {
        "f": "json",
        "where": "municipality_code='00'",
        "returnGeometry": False,
        "spatialRel": "esriSpatialRelIntersects",
        "outFields": "date,vaccine_name,vaccination_state,vaccinated_cum",
        "resultOffset": 0,
        "resultRecordCount": 32000,
        "resultType": "standard",
    }
    res = requests.get(DATA_URL, params=PARAMS)

    data = [elem["attributes"] for elem in json.loads(res.content)["features"]]

    df = pd.DataFrame.from_records(data)

    df["date"] = pd.to_datetime(df["date"], unit="ms")

    # Correction for vaccinations wrongly attributed to early December 2020
    df.loc[df.date < "2020-12-27", "date"] = pd.to_datetime("2020-12-27")

    # Reshape data
    df = df[(df.vaccination_state != "Dalinai") & (df.vaccinated_cum > 0)].copy()
    df.loc[df.vaccination_state == "Visi", "dose_number"] = 1
    df.loc[df.vaccination_state == "Pilnai", "dose_number"] = 2
    df = df.drop(columns="vaccination_state")

    # Data by vaccine
    vaccine_mapping = {
        "Pfizer-BioNTech": "Pfizer/BioNTech",
        "Moderna": "Moderna",
        "AstraZeneca": "Oxford/AstraZeneca",
        "Johnson & Johnson": "Johnson&Johnson",
    }
    vaccines_wrong = set(df["vaccine_name"].unique()).difference(vaccine_mapping)
    if vaccines_wrong:
        raise ValueError(f"Missing vaccines: {vaccines_wrong}")
    # assert set(df["vaccine_name"].unique()) == set(vaccine_mapping.keys())
    df = df.replace(vaccine_mapping)
    vax = (
        df.groupby(["date", "vaccine_name"], as_index=False)["vaccinated_cum"]
        .sum()
        .sort_values("date")
        .rename(
            columns={"vaccine_name": "vaccine", "vaccinated_cum": "total_vaccinations"}
        )
    )
    vax["location"] = "Lithuania"
    vax.to_csv(paths.tmp_vax_out_man("Lithuania"), index=False)
    export_metadata(vax, "Ministry of Health", DATA_URL, paths.tmp_vax_metadata_man)

    # Unpivot
    df = (
        df.groupby(["date", "dose_number", "vaccine_name"], as_index=False)
        .sum()
        .pivot(
            index=["date", "vaccine_name"],
            columns="dose_number",
            values="vaccinated_cum",
        )
        .fillna(0)
        .reset_index()
        .rename(columns={1: "people_vaccinated", 2: "people_fully_vaccinated"})
        .sort_values("date")
    )

    # Total vaccinations
    df = df.assign(total_vaccinations=df.people_vaccinated + df.people_fully_vaccinated)

    # Single shot
    msk = df.vaccine_name == "Johnson & Johnson"
    df.loc[msk, "people_fully_vaccinated"] = df.loc[msk, "people_vaccinated"]

    # Group by date
    df = (
        df.groupby("date")
        .agg(
            {
                "people_fully_vaccinated": sum,
                "people_vaccinated": sum,
                "total_vaccinations": sum,
                "vaccine_name": lambda x: ", ".join(sorted(x)),
            }
        )
        .rename(columns={"vaccine_name": "vaccine"})
        .reset_index()
    )
    df = df.replace(0, pd.NA)

    df.loc[:, "location"] = "Lithuania"
    df.loc[
        :, "source_url"
    ] = "https://experience.arcgis.com/experience/cab84dcfe0464c2a8050a78f817924ca/page/page_3/"

    df.to_csv(paths.tmp_vax_out("Lithuania"), index=False)


if __name__ == "__main__":
    main()
