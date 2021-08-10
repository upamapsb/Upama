import pandas as pd

from cowidev.vax.utils.gsheets import VaccinationGSheet
from cowidev.vax.process import process_location
from cowidev.vax.cmd.utils import get_logger, print_eoe


logger = get_logger()


def main_process_data(
    paths,
    gsheets_api,
    google_spreadsheet_vax_id: str,
    skip_complete: list = None,
    skip_monotonic: dict = {},
    skip_anomaly: dict = {},
):
    print("-- Processing data... --")
    # Get data from sheets
    logger.info("Getting data from Google Spreadsheet...")
    gsheet = VaccinationGSheet(gsheets_api, google_spreadsheet_vax_id)
    df_manual_list = gsheet.df_list()

    # Get automated-country data
    logger.info("Getting data from output...")
    automated = gsheet.automated_countries
    filepaths_auto = [paths.tmp_vax_out(country) for country in automated]
    df_auto_list = [pd.read_csv(filepath) for filepath in filepaths_auto]

    # Concatenate
    vax = df_manual_list + df_auto_list

    # vax = [v for v in vax if v.location.iloc[0] == "Pakistan"]  # DEBUG
    # Process locations
    def _process_location(df):
        monotonic_check_skip = skip_monotonic.get(df.loc[0, "location"], [])
        anomaly_check_skip = skip_anomaly.get(df.loc[0, "location"], [])
        return process_location(df, monotonic_check_skip, anomaly_check_skip)

    logger.info("Processing and exporting data...")
    vax_valid = []
    for df in vax:
        if "location" not in df:
            raise ValueError(f"Column `location` missing. df: {df.tail(5)}")
        country = df.loc[0, "location"]
        if country.lower() not in skip_complete:
            df = _process_location(df)
            vax_valid.append(df)
            # Export
            df.to_csv(paths.pub_vax_loc(country), index=False)
            logger.info(f"{country}: SUCCESS ✅")
        else:
            logger.info(f"{country}: SKIPPED 🚧")
    df = pd.concat(vax_valid).sort_values(by=["location", "date"])
    df.to_csv(paths.tmp_vax_all, index=False)
    gsheet.metadata.to_csv(paths.tmp_met_all, index=False)
    logger.info("Exported ✅")
    print_eoe()
