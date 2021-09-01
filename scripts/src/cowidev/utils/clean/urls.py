import pandas as pd


REGEX_TWITTER = r"(http(?:s)?:\/\/(?:www\.)?twitter\.com\/[a-zA-Z0-9_]+/status/[0-9]+)(\?s=\d+|/photo/\d+)?"
REGEX_FACEBOOK = (
    r"(http(?:s)?:\/\/(?:(?:www|m)\.)?)(facebook\.com\/[a-zA-Z0-9_\.]+\/(?:photos|posts|videos|)\/[0-9\/\.pcba]+)"
    r"((?:\?|__tn__).+)?"
)


def clean_urls(df: pd.DataFrame) -> pd.DataFrame:
    # Twitter
    msk = df.source_url.str.match(REGEX_TWITTER)
    df.loc[msk, "source_url"] = df.loc[msk, "source_url"].str.extract(REGEX_TWITTER)[0]

    # Facebook
    msk = df.source_url.str.fullmatch(REGEX_FACEBOOK)
    df.loc[msk, "source_url"] = "https://www." + df.loc[msk, "source_url"].str.extract(REGEX_FACEBOOK)[1]

    return df
