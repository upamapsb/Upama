"merge"
import pandas as pd


def get_cgrt(bsg_latest: str, country_mapping: str):
    """
    Downloads the latest OxCGRT dataset from BSG's GitHub repository
    Remaps BSG country names to OWID country names

    Returns:
        cgrt {dataframe}
    """

    cgrt = pd.read_csv(bsg_latest, low_memory=False)

    if "RegionCode" in cgrt.columns:
        cgrt = cgrt[cgrt.RegionCode.isnull()]

    cgrt = cgrt[["CountryName", "Date", "StringencyIndex"]]

    cgrt.loc[:, "Date"] = pd.to_datetime(cgrt["Date"], format="%Y%m%d").dt.date.astype(str)

    country_mapping = pd.read_csv(country_mapping)

    cgrt = country_mapping.merge(cgrt, on="CountryName", how="right")

    missing_from_mapping = cgrt[cgrt["Country"].isna()]["CountryName"].unique()
    if len(missing_from_mapping) > 0:
        raise Exception(f"Missing countries in OxCGRT mapping: {missing_from_mapping}")

    cgrt = cgrt.drop(columns=["CountryName"])

    rename_dict = {
        "Country": "location",
        "Date": "date",
        "StringencyIndex": "stringency_index",
    }

    cgrt = cgrt.rename(columns=rename_dict)

    return cgrt
