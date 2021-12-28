import os

import pandas as pd

from cowidev.utils import paths


class CountryTestBase:
    def output_path(self, filename=None):
        if filename is None:
            filename = self.location
        output_path = os.path.join(paths.SCRIPTS.OLD, "testing", "automated_sheets", f"{filename}.csv")
        return output_path

    def export_datafile(self, df, filename=None, attach=False):
        output_path = self.output_path(filename)
        if attach:
            df = merge_with_current_data(df, output_path)
        df.to_csv(output_path, index=False)

    def load_datafile(self, filename=None):
        return pd.read_csv(self.output_path(filename))


def merge_with_current_data(df: pd.DataFrame, filepath: str) -> pd.DataFrame:
    df_current = pd.read_csv(filepath)
    df_current = df_current[df_current.Date < df.Date.min()]
    df = pd.concat([df_current, df]).sort_values("Date")
    return df
