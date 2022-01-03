from cowidev.utils import paths
from cowidev.utils.s3 import df_from_s3


class CountryVaxBase:
    location: str

    def from_ice(self):
        """Loads single CSV `location.csv` from S3 as DataFrame."""
        path = f"{paths.S3.VAX_ICE}/{self.location}.csv"
        df = df_from_s3(path)
        return df
