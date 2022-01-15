from cowidev.utils import paths
from cowidev.utils.s3 import S3, obj_from_s3
from cowidev.utils.clean.dates import localdate

from cowidev.utils import paths


class CountryVaxBase:
    location: str

    def from_ice(self):
        """Loads single CSV `location.csv` from S3 as DataFrame."""
        path = f"{paths.S3.VAX_ICE}/{self.location}.csv"
        _check_last_update(path, self.location)
        df = obj_from_s3(path)
        return df

    @property
    def output_path(self):
        return paths.out_vax(self.location)


def _check_last_update(path, country):
    metadata = S3().get_metadata(path)
    last_update = metadata["LastModified"]
    now = localdate(force_today=True, as_datetime=True)
    num_days = (now - last_update).days
    if num_days > 4:  # Allow maximum 4 days delay
        raise FileExistsError(
            f"ICE File for {country} is too old ({num_days} days old)! Please check cowidev.vax.icer"
        )
