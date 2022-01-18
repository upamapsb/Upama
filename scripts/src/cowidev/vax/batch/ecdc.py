import os

import pandas as pd

from cowidev.utils import paths
from cowidev.utils.clean.dates import clean_date, localdate
from cowidev.utils.utils import check_known_columns
from cowidev.utils.web.download import read_csv_from_url
from cowidev.vax.utils.files import export_metadata_manufacturer, export_metadata_age
from cowidev.vax.utils.orgs import ECDC_VACCINES


AGE_GROUPS_KNOWN = {
    "ALL",
    "Age0_4",
    "Age15_17",
    "Age5_9",
    "Age10_14",
    "Age<18",
    "Age18_24",
    "Age25_49",
    "Age50_59",
    "Age60_69",
    "Age70_79",
    "1_Age<60",
    "1_Age60+",
    "Age80+",
    "AgeUNK",
    "HCW",
    "LTCF",
}


AGE_GROUPS_MUST_HAVE = {
    "Age18_24",
    "Age25_49",
    "Age50_59",
    "Age60_69",
    "Age70_79",
    "Age80+",
}


AGE_GROUP_UNDERAGE_LEVELS = {
    "lvl0": "Age<18",
    "lvl1": {
        "Age0_4",
        "Age5_9",
        "Age10_14",
        "Age15_17",
    },
}


AGE_GROUPS_UNDERAGE = {AGE_GROUP_UNDERAGE_LEVELS["lvl0"]} | AGE_GROUP_UNDERAGE_LEVELS["lvl1"]


AGE_GROUPS_RELEVANT = AGE_GROUPS_UNDERAGE | AGE_GROUPS_MUST_HAVE


LOCATIONS_AGE_EXCLUDED = [
    "Switzerland",
]

LOCATIONS_MANUFACTURER_EXCLUDED = [
    "Czechia",
    "France",
    "Germany",
    "Italy",
    "Latvia",
    "Romania",
    "Iceland",
    "Switzerland",
]


VACCINES_ONE_DOSE = ["JANSS"]


COLUMNS = {
    "Denominator",
    "FirstDose",
    "FirstDoseRefused",
    "NumberDosesReceived",
    "Population",
    "Region",
    "ReportingCountry",
    "SecondDose",
    "TargetGroup",
    "UnknownDose",
    "Vaccine",
    "YearWeekISO",
    "DoseAdditional1",
    "NumberDosesExported",
}


class ECDC:
    def __init__(self, iso_path: str):
        self.source_url = "https://opendata.ecdc.europa.eu/covid19/vaccine_tracker/csv/data.csv"
        self.source_url_ref = "https://www.ecdc.europa.eu/en/publications-data/data-covid-19-vaccination-eu-eea"
        self.country_mapping = self._load_country_mapping(iso_path)
        self.vaccine_mapping = {**ECDC_VACCINES, "UNK": "Unknown"}

    def read(self):
        return read_csv_from_url(self.source_url, timeout=20)

    def _load_country_mapping(self, iso_path: str):
        country_mapping = pd.read_csv(iso_path)
        return dict(zip(country_mapping["alpha-2"], country_mapping["location"]))

    def _weekday_to_date(self, d):
        new_date = clean_date(d + "+5", "%Y-W%W+%w")
        if new_date > localdate("Europe/London"):
            new_date = clean_date(d + "+2", "%Y-W%W+%w")
        return new_date

    def pipe_initial_check(self, df: pd.DataFrame) -> pd.DataFrame:
        # Vaccines
        vaccines_wrong = set(df.Vaccine).difference(self.vaccine_mapping)
        if vaccines_wrong:
            raise ValueError(f"Unknown vaccines found. Check {vaccines_wrong}")
        check_known_columns(df, COLUMNS)
        return df

    def pipe_base(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.pipe(self.pipe_initial_check)
        df = df.assign(
            total_vaccinations=df[["FirstDose", "SecondDose", "UnknownDose", "DoseAdditional1"]].sum(axis=1),
            people_vaccinated=df.FirstDose,
            people_fully_vaccinated=df.SecondDose,
            people_with_booster=df.DoseAdditional1,
            date=df.YearWeekISO.apply(self._weekday_to_date),
            location=df.ReportingCountry.replace(self.country_mapping),
        )
        # Update people_fully_vaccinated
        mask = df.Vaccine.isin(VACCINES_ONE_DOSE)
        df.loc[mask, "people_fully_vaccinated"] = df.loc[mask, "people_fully_vaccinated"] + df.loc[mask, "FirstDose"]
        return df.loc[df.Region.isin(self.country_mapping.keys())]

    def pipe_group(self, df: pd.DataFrame, group_field: str, group_field_renamed: str) -> pd.DataFrame:
        return (
            df.groupby(["date", "location", group_field], as_index=False)[
                [
                    "total_vaccinations",
                    "people_vaccinated",
                    "people_fully_vaccinated",
                    "people_with_booster",
                    "UnknownDose",
                ]
            ]
            .sum()
            .rename(columns={group_field: group_field_renamed})
        )

    def pipe_cumsum(self, df: pd.DataFrame, group_field_renamed: str) -> pd.DataFrame:
        return df.assign(
            total_vaccinations=df.groupby(["location", group_field_renamed])["total_vaccinations"].cumsum(),
            people_vaccinated=df.groupby(["location", group_field_renamed])["people_vaccinated"].cumsum(),
            people_fully_vaccinated=df.groupby(["location", group_field_renamed])["people_fully_vaccinated"].cumsum(),
            people_with_booster=df.groupby(["location", group_field_renamed])["people_with_booster"].cumsum(),
            UnknownDose=df.groupby(["location", group_field_renamed])["UnknownDose"].cumsum(),
        )

    def pipeline_common(self, df: pd.DataFrame, group_field: str, group_field_renamed: str) -> pd.DataFrame:
        return (
            df.pipe(self.pipe_group, group_field, group_field_renamed)[
                [
                    "date",
                    "location",
                    group_field_renamed,
                    "total_vaccinations",
                    "people_vaccinated",
                    "people_fully_vaccinated",
                    "people_with_booster",
                    "UnknownDose",
                ]
            ]
            .sort_values("date")
            .pipe(self.pipe_cumsum, group_field_renamed)
        )

    def pipe_rename_vaccines(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(vaccine=df.vaccine.replace(self.vaccine_mapping))

    def pipe_manufacturer_filter_locations(self, df: pd.DataFrame):
        """Filters countries to be excluded and those with a high number of unknown doses."""

        def _get_perc_unk(x):
            res = x.groupby("vaccine").total_vaccinations.sum()
            res /= res.sum()
            if not "Unknown" in res:
                return 0
            return res.loc["Unknown"]

        threshold_unk_ratio = 0.05
        mask = df.groupby("location").apply(_get_perc_unk) < threshold_unk_ratio
        locations_valid = mask[mask].index.tolist()
        locations_valid = [loc for loc in locations_valid if loc not in LOCATIONS_MANUFACTURER_EXCLUDED]
        df = df[df.location.isin(locations_valid)]
        return df

    def pipe_manufacturer_filter_entries(self, df: pd.DataFrame):
        return df[~df.vaccine.isin(["Unknown"])]

    def pipeline_manufacturer(self, df: pd.DataFrame):
        group_field_renamed = "vaccine"
        return (
            df.loc[df.TargetGroup == "ALL"]
            .pipe(self.pipeline_common, "Vaccine", group_field_renamed)
            .pipe(self.pipe_rename_vaccines)
            .pipe(self.pipe_manufacturer_filter_locations)
            .pipe(self.pipe_manufacturer_filter_entries)[["location", "date", "vaccine", "total_vaccinations"]]
            .sort_values(["location", "date", "vaccine"])
        )

    def pipe_age_checks(self, df: pd.DataFrame) -> pd.DataFrame:
        # Check all age groups are valid names
        ages_groups_wrong = set(df.age_group).difference(AGE_GROUPS_KNOWN)
        if ages_groups_wrong:
            raise ValueError(f"Unknown age groups found. Check {ages_groups_wrong}")
        return df

    def pipe_age_filter_locations(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter locations and keep only valid ones.

        Validity is defined as a country having all age groups defined by `AGE_GROUPS_MUST_HAVE`.
        """
        locations = df.location.unique()
        locations_valid = []
        for location in locations:
            df_c = df.loc[df.location == location]
            if not AGE_GROUPS_MUST_HAVE.difference(df_c.age_group.unique()):
                locations_valid.append(location)
        locations_valid = [loc for loc in locations_valid if loc not in LOCATIONS_AGE_EXCLUDED]
        df = df[df.location.isin(locations_valid)]
        return df

    def pipe_age_filter_entries(self, df: pd.DataFrame) -> pd.DataFrame:
        """More granular filter. Keep entries where data is deemed reliable.

        1. Checks field ALL is equal to sum of all other ages (within 5% error). If not filters rows out.
        2. If percentage of unknown doses is above 5% of total doses, filters row out.
        """
        # Find valid dates + location
        x = df.pivot(index=["date", "location"], columns="age_group", values="total_vaccinations").reset_index()
        x = x.dropna(subset=AGE_GROUPS_MUST_HAVE, how="any")
        # Create debug variable (= sum of all ages)
        x = x.assign(
            debug_u18=x[AGE_GROUP_UNDERAGE_LEVELS["lvl0"]].fillna(x[AGE_GROUP_UNDERAGE_LEVELS["lvl1"]].sum(axis=1))
        )
        x = x.assign(debug=x[AGE_GROUPS_MUST_HAVE].sum(axis=1) + x.debug_u18)
        x = x.assign(
            debug_diff=x.ALL - x.debug,
            debug_diff_perc=(x.ALL - x.debug) / x.ALL,
        )
        threshold_missmatch_ratio = 0.05  # Keep only those days where missmatch between sum(ages) and total is <5%
        x = x[x.debug_diff_perc <= threshold_missmatch_ratio]
        valid_entries_ids = x[["date", "location"]]
        if not valid_entries_ids.value_counts().max() == 1:
            raise ValueError("Some entries appear to be duplicated")
        df = df.merge(valid_entries_ids, on=["date", "location"])

        # Filter entries with too many unknown doses (where more 5% of doses are unknown)
        threshold_unknown_doses_ratio = 0.05
        df = df[(df.UnknownDose / df.total_vaccinations) < threshold_unknown_doses_ratio]
        return df

    def pipe_age_groups(self, df: pd.DataFrame) -> pd.DataFrame:
        """Build age groups."""
        # df = df[~df.age_group.isin(['LTCF', 'HCW', 'AgeUNK', 'ALL'])]
        df_ = df[df.age_group.isin(AGE_GROUPS_RELEVANT)].copy()
        df_ = df_.assign(age_group_modified=df_.age_group.replace({"Age<18": "Age0_17"}))
        regex = r"(?:1_)?Age(\d{1,2})?(?:\+|<)?_?(\d{1,2})?"
        df_[["age_group_min", "age_group_max"]] = df_.age_group_modified.str.extract(regex)
        # df_ = df_.assign(age_group_min=df_.age_group_min.fillna(0))
        # df.loc[df.age_group == "1_Age60+", ["age_group_min", "age_group_max"]] = [60, pd.NA]
        # df.loc[df.age_group == "1_Age<60", ["age_group_min", "age_group_max"]] = [0, 60]
        return df_

    def pipe_age_relative_metrics(self, df: pd.DataFrame, df_og: pd.DataFrame) -> pd.DataFrame:
        df_den = df_og.loc[df_og.TargetGroup.isin(AGE_GROUPS_RELEVANT)].dropna(subset=["Denominator"])
        if df_den.Denominator.isnull().any():
            raise ValueError(f"Denomintor found to be null: {df_den[df_den.Denominator.isnull()]}")
        res = df_den.groupby(["date", "location", "TargetGroup"]).Denominator.nunique()
        if (res != 1).any():
            raise ValueError(
                "Several Denomintor values found for same (date, location, TargetGroup):"
                f" {res[res== 1].index.tolist()}"
            )
        df_den = df_den[["date", "location", "TargetGroup", "Denominator"]].drop_duplicates()
        df = df.merge(
            df_den,
            left_on=["date", "age_group", "location"],
            right_on=["date", "TargetGroup", "location"],
        )
        return df.assign(
            people_vaccinated_per_hundred=(100 * df.people_vaccinated / df.Denominator).round(2),
            people_fully_vaccinated_per_hundred=(100 * df.people_fully_vaccinated / df.Denominator).round(2),
            people_with_booster_per_hundred=(100 * df.people_with_booster / df.Denominator).round(2),
        )

    def pipeline_age(self, df: pd.DataFrame):
        group_field_renamed = "age_group"
        return (
            df
            # .dropna(subset=["Denominator"])
            .pipe(self.pipeline_common, "TargetGroup", group_field_renamed)
            .pipe(self.pipe_age_checks)
            .pipe(self.pipe_age_filter_locations)
            .pipe(self.pipe_age_filter_entries)
            .pipe(self.pipe_age_groups)
            .pipe(self.pipe_age_relative_metrics, df)
            .drop(columns=[group_field_renamed])
            .sort_values(["location", "date", "age_group_min"])
        )

    def _filter_age_targetgroup(self, df_c: pd.DataFrame):
        # Filter age groups
        date_0 = df_c.loc[df_c.TargetGroup.isin({AGE_GROUP_UNDERAGE_LEVELS["lvl0"]}), "date"].unique()
        date_1 = df_c.loc[df_c.TargetGroup.isin(AGE_GROUP_UNDERAGE_LEVELS["lvl1"]), "date"].unique()
        if (len(date_0) == len(date_1)) | (len(date_0) == 0):
            age_group_selection = AGE_GROUPS_MUST_HAVE | AGE_GROUP_UNDERAGE_LEVELS["lvl1"]
        elif len(date_1) == 0:
            age_group_selection = AGE_GROUPS_MUST_HAVE | {AGE_GROUP_UNDERAGE_LEVELS["lvl0"]}
        else:
            raise ValueError(
                f"Can't choose between under age groups. Restriction might be too strict, consider relaxing it!"
            )
        df_c = df_c[df_c.TargetGroup.isin(age_group_selection)]
        return df_c

    def export_age(self, df: pd.DataFrame):
        df_age = df.pipe(self.pipeline_age)
        # Export
        locations = df_age.location.unique()
        for location in locations:
            df_c = df_age[df_age.location == location].pipe(self._filter_age_targetgroup)
            df_c.to_csv(
                paths.out_vax(location, age=True),
                columns=[
                    "location",
                    "date",
                    "age_group_min",
                    "age_group_max",
                    "people_vaccinated_per_hundred",
                    "people_fully_vaccinated_per_hundred",
                    "people_with_booster_per_hundred",
                ],
                index=False,
            )
        export_metadata_age(
            df=df,
            source_name="European Centre for Disease Prevention and Control (ECDC)",
            source_url=self.source_url_ref,
        )

    def export_manufacturer(self, df: pd.DataFrame):
        df_manufacturer = df.pipe(self.pipeline_manufacturer)
        # Export
        locations = df_manufacturer.location.unique()
        for location in locations:
            df_c = df_manufacturer[df_manufacturer.location == location]
            df_c.to_csv(
                paths.out_vax(location, manufacturer=True),
                columns=["location", "date", "vaccine", "total_vaccinations"],
                index=False,
            )
        export_metadata_manufacturer(
            df=df_manufacturer,
            source_name="European Centre for Disease Prevention and Control (ECDC)",
            source_url=self.source_url_ref,
        )

    def export(self):
        # Read data
        df = self.read().pipe(self.pipe_base)
        # Age
        self.export_age(df)
        # Manufacturer
        self.export_manufacturer(df)


def main():
    ECDC(iso_path=os.path.join(paths.SCRIPTS.INPUT_ISO, "iso.csv")).export()
