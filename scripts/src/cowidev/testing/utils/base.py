import os
from cowidev.utils import paths


class CountryTestBase:
    def export_datafile(self, df, filename=None):
        if filename is None:
            filename = self.location
        output_path = os.path.join(paths.SCRIPTS.OLD, "testing", "automated_sheets", f"{filename}.csv")
        df.to_csv(output_path, index=False)
