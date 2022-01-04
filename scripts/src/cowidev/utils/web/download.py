import requests
import tempfile
import pandas as pd


def read_xlsx_from_url(url: str, timeout=30, as_series: bool = False, drop=False, **kwargs) -> pd.DataFrame:
    """Download and load xls file from URL.

    Args:
        url (str): File url.
        as_series (bol): Set to True to return a pandas.Series object. Source file must be of shape 1xN (1 row, N
                            columns). Defaults to False.
        kwargs: Arguments for pandas.read_excel.

    Returns:
        pandas.DataFrame: Data loaded.
    """
    with tempfile.NamedTemporaryFile() as tmp:
        download_file_from_url(url, tmp.name, timeout=timeout)
        df = pd.read_excel(tmp.name, **kwargs)
    if as_series:
        return df.T.squeeze()
    if drop:
        df = df.dropna(how="all")
    return df


def read_csv_from_url(url, timeout=30, **kwargs):
    with tempfile.NamedTemporaryFile(mode="w+", delete=False) as tmp:
        download_file_from_url(url, tmp.name, timeout=timeout)
        df = pd.read_csv(tmp.name, **kwargs)
    # df = df.dropna(how="all")
    return df


def download_file_from_url(url, save_path, chunk_size=1024 * 1024, timeout=30):
    r = requests.get(url, stream=True, timeout=timeout)
    with open(save_path, "wb") as fd:
        for chunk in r.iter_content(chunk_size=chunk_size):
            fd.write(chunk)
