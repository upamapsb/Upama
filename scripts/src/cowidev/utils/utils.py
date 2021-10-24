import os
import pytz
import ntpath
import tempfile

from datetime import datetime, timedelta
from dotenv import load_dotenv
from xlsx2csv import Xlsx2csv

from cowidev.utils.web.download import download_file_from_url


def get_project_dir(err: bool = False):
    load_dotenv()
    project_dir = os.environ.get("OWID_COVID_PROJECT_DIR")
    if project_dir is None:  # err and
        raise ValueError("Please have ${OWID_COVID_PROJECT_DIR}.")
    return project_dir


def export_timestamp(timestamp_filename: str):
    timestamp_filename = os.path.join(get_project_dir(), "public", "data", "internal", "timestamp", timestamp_filename)
    with open(timestamp_filename, "w") as timestamp_file:
        timestamp_file.write(datetime.utcnow().replace(microsecond=0).isoformat())


def time_str_grapher():
    return (
        (datetime.now() - timedelta(minutes=10))
        .astimezone(pytz.timezone("Europe/London"))
        .strftime("%-d %B %Y, %H:%M")
    )


def get_filename(filepath: str, remove_extension: bool = True):
    filename = ntpath.basename(filepath)
    if remove_extension:
        return filename.split(".")[0]
    return filename


def xlsx2csv(filename_xlsx: str, filename_csv: str):
    if filename_xlsx.startswith("https://") or filename_xlsx.startswith("http://"):
        with tempfile.NamedTemporaryFile() as tmp:
            download_file_from_url(filename_xlsx, tmp.name)
            Xlsx2csv(tmp.name, outputencoding="utf-8").convert(filename_csv)
    else:
        Xlsx2csv(filename_xlsx, outputencoding="utf-8").convert(filename_csv)


def pd_series_diff_values(a, b):
    common = set(a) & set(b)
    return {*set(a[-a.isin(common)]), *set(b[-b.isin(common)])}
