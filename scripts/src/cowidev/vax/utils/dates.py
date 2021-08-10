from datetime import datetime, timedelta
from contextlib import contextmanager
import locale

import threading
from sys import platform
import pytz
import unicodedata
import re

import pandas as pd


LOCALE_LOCK = threading.Lock()
DATE_FORMAT = "%Y-%m-%d"


def clean_date(date_or_text, fmt=None, lang=None, loc="", minus_days=0):
    """Extract a date from a `text`.

    The date from text is extracted using locale `loc`. Alternatively, you can provide language `lang` instead.

    By default, system default locale is used.

    Args:
        date_or_text (str): Input text or date.
        fmt (str, optional): Text format. More details at
                             https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes.
        lang (str, optional): Language two-letter code, e.g. 'da' (dansk). If given, `loc` will be ignored and redefined
                                based on `lang`. Defaults to None.
        loc (str, optional): Locale, e.g es_ES. Get list of available locales with `locale.locale_alias` or
                                `locale.windows_locale` in windows. Defaults to "" (system default).
        minus_days (int, optional): Number of days to subtract. Defaults to 0.

    Returns:
        str: Extracted date in format %Y-%m-%d
    """
    if isinstance(date_or_text, datetime):
        return date_or_text.strftime(DATE_FORMAT)
    # If lang is given, map language to a locale
    if fmt is None:
        raise ValueError("Input date format is required!")
    if lang is not None:
        if lang in locale.locale_alias:
            loc = locale.locale_alias[lang]
    if platform == "win32":
        if loc is not None:
            loc = loc.replace("_", "-")
    # Thread-safe extract date
    with _setlocale(loc):
        return (
            datetime.strptime(date_or_text, fmt) - timedelta(days=minus_days)
        ).strftime(DATE_FORMAT)


def extract_clean_date(
    text: str,
    regex: str,
    date_format: str,
    lang: str = None,
    loc: str = "",
    minus_days: int = 0,
    unicode_norm: bool = True,
    replace_year=None,
):
    """Export clean date from raw text using RegEx.

    Example:

    ```python
    >>> from cowidev.vax.utils.utils import extract_clean_date
    >>> text = "Something irrelevant. This page was last updated on 25 May 2021 at 09:05hrs."
    >>> date_str = extract_clean_date(
        text=text,
        regex=r"This page was last updated on (\d{1,2} May 202\d) at \d{1,2}:\d{1,2}hrs",
        date_format="%d %B %Y",
        minus_days=1,
    )
    ```

    Args:
        text (str): Raw original text.
        regex (str): RegEx to export date fragment. Should have the data grouped (group number 1)
        date_format (str): Format of the date (was extracted using regex).
        lang (str, optional): Language two-letter code, e.g. 'da' (dansk). If given, `loc` will be ignored and redefined
                                based on `lang`. Defaults to None.
        loc (str, optional): Locale, e.g es_ES. Get list of available locales with `locale.locale_alias` or
                                `locale.windows_locale` in windows. Defaults to "" (system default).
        minus_days (int, optional): Number of days to subtract. Defaults to 0.
        unicode_norm (bool, optional): [description]. Defaults to True.
        replace_year (str): Replace the year with this one.
    """
    if unicode_norm:
        text = unicodedata.normalize("NFKC", text)
    date_raw = re.search(regex, text).group(1)
    date_str = clean_date(
        date_raw, fmt=date_format, lang=lang, loc=loc, minus_days=minus_days
    )
    if replace_year is not None:
        date_str = _replace_date_fields(date_str, {"year": replace_year})
    return date_str


def _replace_date_fields(date_raw: str, replace_fields: dict = {}, date_format: str = DATE_FORMAT):
    """Replace date field.

    Args:
        date_raw (str): Date raw in standard format %Y-%m-%d.
        replace_fields (dict, optional): Fields to replace. Format should be: dict(field, value), e.g. {year: "2021"}.
        date_format (str, optional): Date format of `date_raw`. Defaults to DATE_FORMAT.

    Returns:
        str: Modified date, in standard format %Y-%m-%d.

    """
    dt = datetime.strptime(date_raw, date_format)
    dt = dt.replace(**replace_fields)
    return dt.strftime(DATE_FORMAT)

def localdatenow(tz=None):
    if tz is None:
        tz = "utc"
    return localdate(tz, 0)


def localdate(tz, hour_limit=None, date_format=None):
    """Get local date.

    By default, gets date prior to execution.

    Args:
        tz (str, optional): Timezone name.
        hour_limit (int, optional): If local time hour is lower than this, returned date is previous day.
                                    Defaults to None.
        date_format (str, optional): Format of output datetime. Uses default YYYY-mm-dd.
    """
    tz = pytz.timezone(tz)
    local_time = datetime.now(tz=tz)
    if (hour_limit is None) or (local_time.hour < hour_limit):
        local_time = local_time - timedelta(days=1)
    if date_format is None:
        date_format = DATE_FORMAT
    return local_time.strftime(date_format)


def clean_date_series(
    ds: pd.Series, format_input: str = None, format_output: str = "%Y-%m-%d"
) -> pd.Series:
    if format_output is None:
        format_output = DATE_FORMAT
    return pd.to_datetime(ds, format=format_input).dt.strftime(format_output)


@contextmanager
def _setlocale(name):
    # REF: https://stackoverflow.com/questions/18593661/how-do-i-strftime-a-date-object-in-a-different-locale
    with LOCALE_LOCK:
        saved = locale.setlocale(locale.LC_TIME, "")
        try:
            yield locale.setlocale(locale.LC_TIME, name)
        finally:
            locale.setlocale(locale.LC_TIME, saved)


def from_tz_to_tz(dt, from_tz: str = "UTC", to_tz: str = None):
    dt = dt.replace(tzinfo=pytz.timezone(from_tz))
    dt = dt.astimezone(pytz.timezone(to_tz))
    return dt
