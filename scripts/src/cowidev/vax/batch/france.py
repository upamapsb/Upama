from cowidev.utils import paths
from cowidev.utils.utils import check_known_columns
from cowidev.utils.web.download import read_csv_from_url
from cowidev.vax.utils.files import export_metadata_manufacturer
from cowidev.vax.utils.utils import build_vaccine_timeline


def main():

    vaccine_mapping = {
        1: "Pfizer/BioNTech",
        2: "Moderna",
        3: "Oxford/AstraZeneca",
        4: "Johnson&Johnson",
    }
    one_dose_vaccines = ["Johnson&Johnson"]

    source = "https://www.data.gouv.fr/fr/datasets/r/b273cf3b-e9de-437c-af55-eda5979e92fc"

    df = read_csv_from_url(source, sep=";")
    check_known_columns(
        df,
        [
            "fra",
            "vaccin",
            "jour",
            "n_dose1",
            "n_dose2",
            "n_dose3",
            "n_dose4",
            "n_rappel",
            "n_cum_dose1",
            "n_cum_dose2",
            "n_cum_dose3",
            "n_cum_dose4",
            "n_cum_rappel",
        ],
    )
    df = df[["vaccin", "jour", "n_cum_dose1", "n_cum_dose2", "n_cum_dose3", "n_cum_dose4"]]

    df = df.rename(
        columns={
            "vaccin": "vaccine",
            "jour": "date",
        }
    )

    # Map vaccine names
    df = df[(df.vaccine.isin(vaccine_mapping.keys())) & (df.n_cum_dose1 > 0)]
    assert set(df["vaccine"].unique()) == set(vaccine_mapping.keys())
    df["vaccine"] = df.vaccine.replace(vaccine_mapping)

    df["total_vaccinations"] = df.n_cum_dose1 + df.n_cum_dose2 + df.n_cum_dose3 + df.n_cum_dose4
    df["people_vaccinated"] = df.n_cum_dose1

    # 2-dose vaccines
    mask = -df.vaccine.isin(one_dose_vaccines)
    df.loc[mask, "people_fully_vaccinated"] = df.n_cum_dose2
    df.loc[mask, "total_boosters"] = df.n_cum_dose3 + df.n_cum_dose4

    # 1-dose vaccines
    mask = df.vaccine.isin(one_dose_vaccines)
    df.loc[mask, "people_fully_vaccinated"] = df.n_cum_dose1
    df.loc[mask, "total_boosters"] = df.n_cum_dose2 + df.n_cum_dose3 + df.n_cum_dose4

    df = df.drop(columns=["n_cum_dose1", "n_cum_dose2", "n_cum_dose3", "n_cum_dose4"])

    manufacturer = df[["date", "total_vaccinations", "vaccine"]].assign(location="France")
    manufacturer.to_csv(paths.out_vax("France", manufacturer=True), index=False)
    export_metadata_manufacturer(manufacturer, "Public Health France", source)
    approval_timeline = manufacturer[["vaccine", "date"]].groupby("vaccine").min().to_dict()["date"]

    df = (
        df.groupby("date", as_index=False)
        .agg(
            {
                "total_vaccinations": "sum",
                "people_vaccinated": "sum",
                "people_fully_vaccinated": "sum",
                "total_boosters": "sum",
            }
        )
        .pipe(build_vaccine_timeline, approval_timeline)
    )

    df = df.assign(
        location="France",
        source_url=(
            "https://www.data.gouv.fr/fr/datasets/donnees-relatives-aux-personnes-vaccinees-contre-la-covid-19-1/"
        ),
    )

    df.to_csv(paths.out_vax("France"), index=False)


if __name__ == "__main__":
    main()
