import os

import pandas as pd

from cowidev.utils import paths


class CountryTestBase:
    def export_datafile(self, df, filename=None, attach=False):
        if filename is None:
            filename = self.location
        output_path = os.path.join(paths.SCRIPTS.OLD, "testing", "automated_sheets", f"{filename}.csv")
        if attach:
            df = merge_with_current_data(df, output_path)
        df.to_csv(output_path, index=False)


def merge_with_current_data(df: pd.DataFrame, filepath: str) -> pd.DataFrame:
    df_current = pd.read_csv(filepath)
    df_current = df_current[df_current.Date < df.Date.min()]
    df = pd.concat([df_current, df]).sort_values("Date")
    return df
