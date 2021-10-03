import requests
import tempfile
import pandas as pd


def read_xlsx_from_url(url: str, as_series: bool = False, **kwargs) -> pd.DataFrame:
    """Download and load xls file from URL.

    Args:
        url (str): File url.
        as_series (bol): Set to True to return a pandas.Series object. Source file must be of shape 1xN (1 row, N
                            columns). Defaults to False.
        kwargs: Arguments for pandas.read_excel.

    Returns:
        pandas.DataFrame: Data loaded.
    """
    headers = {"User-Agent": "Mozilla/5.0 (X11; Linux i686)"}
    response = requests.get(url, headers=headers)
    with tempfile.NamedTemporaryFile() as tmp:
        with open(tmp.name, "wb") as f:
            f.write(response.content)
        df = pd.read_excel(tmp.name, **kwargs)
    if as_series:
        return df.T.squeeze()
    return df


def download_file_from_url(url, save_path, chunk_size=128):
    r = requests.get(url, stream=True)
    with open(save_path, "wb") as fd:
        for chunk in r.iter_content(chunk_size=chunk_size):
            fd.write(chunk)
