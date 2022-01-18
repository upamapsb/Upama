import unicodedata


def clean_string(text_raw: str):
    """Clean column name."""
    text_new = unicodedata.normalize("NFKC", text_raw).strip()
    return text_new
