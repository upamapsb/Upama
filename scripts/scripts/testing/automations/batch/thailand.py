import re
import requests
import datetime
import pandas as pd
from bs4 import BeautifulSoup


def main():

    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36"
    }
    general_url = "https://www3.dmsc.moph.go.th/"

    req = requests.get(general_url, headers=headers)
    soup = BeautifulSoup(req.content, "html.parser")

    url = soup.find("div", class_="container-fluid").find_all("a")[3].attrs["href"]

    sheet = pd.read_excel(url + "/download", sheet_name="Data", usecols="A,B,C")

    isdate = []
    for i in range(len(sheet)):
        isdate.append(isinstance(sheet.loc[i][0], datetime.datetime))
    isdate
    sheet["isdate"] = isdate
    sheet = sheet.loc[sheet["isdate"] != False]
    sheet = sheet.loc[sheet["Total"] != 0]

    sheet["Date"] = pd.to_datetime(sheet["Date"], errors="coerce")
    sheet["Date"] = sheet["Date"].dt.strftime("%Y-%m-%d")

    sheet["Total"] = sheet["Total"].astype(int)

    sheet = sheet.drop(columns=["Pos", "isdate"])
    sheet = sheet.rename(columns={"Total": "Daily change in cumulative total"})

    sheet.loc[:, "Country"] = "Thailand"
    sheet.loc[:, "Units"] = "tests performed"
    sheet.loc[:, "Source URL"] = general_url
    sheet.loc[
        :, "Source label"
    ] = "Department of Medical Sciences Ministry of Public Health"
    sheet.loc[:, "Notes"] = pd.NA

    sheet.to_csv("automated_sheets/Thailand.csv", index=False)


if __name__ == "__main__":
    main()
