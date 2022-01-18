"""Contains methods importing input directory files.

Input directory is found at `[project-dir]/scripts/input`.

Example usage:

```
python import_inputs.py wb-income-groups
```
"""


import os
import argparse

import pandas as pd


def _parse_args():
    parser = argparse.ArgumentParser(
        description="Import necessary input files.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "dataset",
        choices=["wb-income-groups", "all"],
        default="all",
        help=("Choose which data to import."),
    )
    parser.add_argument(
        "--input-dir",
        default=os.path.abspath(os.path.join("..", "input")),
        help=("Path to folder containing input."),
    )
    args = parser.parse_args()
    return args


def import_wb_income_groups(output: str, url: str = None):
    if url is None:
        url = "http://databank.worldbank.org/data/download/site-content/CLASS.xls"
    df_wb = (
        pd.read_excel(
            url,
            skiprows=3,
            header=1,
            nrows=219,
            usecols=["Economy", "Code", "Income group"],
        )
        .drop(0)
        .rename(columns={"Economy": "Country"})
        .assign(Year=2020)
    )
    replace_countries = {
        "Bahamas, The": "Bahamas",
        "Brunei Darussalam": "Brunei",
        "Cabo Verde": "Cape Verde",
        "Congo, Dem. Rep.": "Democratic Republic of Congo",
        "Congo, Rep.": "Congo",
        "Curaçao": "Curacao",
        "Czech Republic": "Czechia",
        "Côte d'Ivoire": "Cote d'Ivoire",
        "Egypt, Arab Rep.": "Egypt",
        "Faroe Islands": "Faeroe Islands",
        "Gambia, The": "Gambia",
        "Hong Kong SAR, China": "Hong Kong",
        "Iran, Islamic Rep.": "Iran",
        "Korea, Dem. People's Rep.": "North Korea",
        "Korea, Rep.": "South Korea",
        "Kyrgyz Republic": "Kyrgyzstan",
        "Lao PDR": "Laos",
        "Macao SAR, China": "Macao",
        "Micronesia, Fed. Sts.": "Micronesia (country)",
        "Russian Federation": "Russia",
        "Slovak Republic": "Slovakia",
        "St. Kitts and Nevis": "Saint Kitts and Nevis",
        "St. Lucia": "Saint Lucia",
        "St. Martin (French part)": "Saint Martin (French part)",
        "St. Vincent and the Grenadines": "Saint Vincent and the Grenadines",
        "São Tomé and Principe": "Sao Tome and Principe",
        "Syrian Arab Republic": "Syria",
        "Taiwan, China": "Taiwan",
        "Timor-Leste": "Timor",
        "Venezuela, RB": "Venezuela",
        "Virgin Islands (U.S.)": "United States Virgin Islands",
        "West Bank and Gaza": "Palestine",
        "Yemen, Rep.": "Yemen",
    }
    replace_income_groups = {
        "High income": "High-income countries",
        "Upper middle income": "Upper-middle-income countries",
        "Lower middle income": "Lower-middle-income countries",
        "Low income": "Low-income countries",
    }
    df_wb.loc[df_wb.Country == "Kosovo", "Code"] = "OWID_KOS"  # Legacy
    df_wb = df_wb.assign(Country=df_wb.Country.replace(replace_countries))
    df_wb["Income group"] = df_wb["Income group"].replace(replace_income_groups)
    df_wb.to_csv(
        os.path.join(output, "wb", "income_groups.csv"), index=False
    )  # Folder 'wb' assumed to exist!

    # Additional
    df_extra = pd.DataFrame(
        [
            # ["England","OWID_ENG",2020,"High income"],
            # ["Scotland","OWID_SCT",2020,"High income"],
            # ["Wales","OWID_WLS",2020,"High income"],
            # ["Northern Ireland","OWID_NIR",2020,"High income"],
            ["Falkland Islands", "FLK", 2020, "High income"],
            ["Guernsey", "GGY", 2020, "High income"],
            ["Jersey", "JEY", 2020, "High income"],
            ["Saint Helena", "SHN", 2020, "High income"],
            ["Montserrat", "MSR", 2020, "High income"],
            ["Northern Cyprus", "OWID_CYN", 2020, "High income"],
            ["Wallis and Futuna", "WLF", 2020, "High income"],
            ["Anguilla", "AIA", 2020, "High income"],
        ],
        columns=["Country", "Code", "Year", "Income group"],
    )
    df_extra.to_csv(
        os.path.join(output, "owid", "income_groups_complement.csv"), index=False
    )


def main():
    args = _parse_args()
    if args.dataset == "wb-income-groups":
        import_wb_income_groups(args.input_dir)
    elif args.dataset == "all":
        import_wb_income_groups(args.input_dir)


if __name__ == "__main__":
    main()
