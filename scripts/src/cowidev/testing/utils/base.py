import os
from cowidev.utils import paths


class CountryTestBase:
    def export_datafile(self, df):
        output_path = os.path.join(paths.SCRIPTS.OLD, "testing", "automated_sheets", f"{self.location}.csv")
        df.to_csv(output_path, index=False)
