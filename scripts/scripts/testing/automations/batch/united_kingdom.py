import pandas as pd
import requests
from datetime import datetime
from uk_covid19 import Cov19API


def main():

    # England (Include PillarTwo from 2 July 2020)
    filters = ["areaType=Nation", "areaName=England"]
    structure = {
        "Date": "date",
        "Country": "areaName",
        "cumPillarOne": "cumPillarOneTestsByPublishDate",
        "newPillarTwo": "newPillarTwoTestsByPublishDate",
    }
    api = Cov19API(filters=filters, structure=structure)
    england = api.get_dataframe()

    england["cumPillarTwo"] = (
        england[pd.to_datetime(england["Date"]) > "2020-07-01"]["newPillarTwo"][::-1]
        .cumsum()
        .fillna(method="ffill")
    )
    england["Cumulative total"] = england["cumPillarOne"] + england[
        "cumPillarTwo"
    ].fillna(0)

    # N ireland (Include PillarTwo from 26 June 2020)
    filters = ["areaType=Nation", "areaName=Northern Ireland"]
    structure = {
        "Date": "date",
        "Country": "areaName",
        "cumPillarOne": "cumPillarOneTestsByPublishDate",
        "newPillarTwo": "newPillarTwoTestsByPublishDate",
    }
    api = Cov19API(filters=filters, structure=structure)
    nireland = api.get_dataframe()

    nireland["cumPillarTwo"] = (
        nireland[pd.to_datetime(nireland["Date"]) > "2020-06-25"]["newPillarTwo"][::-1]
        .cumsum()
        .fillna(method="ffill")
    )
    nireland["Cumulative total"] = nireland["cumPillarOne"] + nireland[
        "cumPillarTwo"
    ].fillna(0)

    # Scotland (Include PillarTwo from 15 June 2020)
    filters = ["areaType=Nation", "areaName=Scotland"]
    structure = {
        "Date": "date",
        "Country": "areaName",
        "cumPillarOne": "cumPillarOneTestsByPublishDate",
        "newPillarTwo": "newPillarTwoTestsByPublishDate",
    }
    api = Cov19API(filters=filters, structure=structure)
    scotland = api.get_dataframe()

    scotland["cumPillarTwo"] = (
        scotland[pd.to_datetime(scotland["Date"]) > "2020-06-14"]["newPillarTwo"][::-1]
        .cumsum()
        .fillna(method="ffill")
    )
    scotland["Cumulative total"] = scotland["cumPillarOne"] + scotland[
        "cumPillarTwo"
    ].fillna(0)

    # Wales (Include PillarTwo from 14 July 2020)
    filters = ["areaType=Nation", "areaName=Wales"]
    structure = {
        "Date": "date",
        "Country": "areaName",
        "cumPillarOne": "cumPillarOneTestsByPublishDate",
        "newPillarTwo": "newPillarTwoTestsByPublishDate",
    }
    api = Cov19API(filters=filters, structure=structure)
    wales = api.get_dataframe()

    wales["cumPillarTwo"] = (
        wales[pd.to_datetime(wales["Date"]) > "2020-07-13"]["newPillarTwo"][::-1]
        .cumsum()
        .fillna(method="ffill")
    )
    wales["Cumulative total"] = wales["cumPillarOne"] + wales["cumPillarTwo"].fillna(0)

    countries = [england, nireland, scotland, wales]
    uk = pd.concat(countries).sort_values("Date")
    uk = uk.groupby("Date", as_index=False).agg({"Cumulative total": "sum"})

    uk["Country"] = "United Kingdom"
    uk["Source URL"] = "https://coronavirus.data.gov.uk/details/testing"
    uk["Source label"] = "Public Health England"
    uk["Units"] = "tests performed"
    uk[
        "Notes"
    ] = "PillarOne: England, N. Ireland, Scotland, Wales; PillarTwo: England, N. Ireland, Scotland, Wales"

    uk.loc[
        uk.Date < "2020-06-15", "Notes"
    ] = "PillarOne: England, N. Ireland, Scotland, Wales; PillarTwo: None"
    uk.loc[
        uk.Date < "2020-06-26", "Notes"
    ] = "PillarOne: England, N. Ireland, Scotland, Wales; PillarTwo: Scotland"
    uk.loc[
        uk.Date < "2020-07-02", "Notes"
    ] = "PillarOne: England, N. Ireland, Scotland, Wales; PillarTwo: N. Ireland, Scotland"
    uk.loc[
        uk.Date < "2020-07-14", "Notes"
    ] = "PillarOne: England, N. Ireland, Scotland, Wales; PillarTwo: England, N. Ireland, Scotland"

    uk.to_csv("automated_sheets/United Kingdom.csv", index=False)


if __name__ == "__main__":
    main()
