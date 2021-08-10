"""Clean and process url fields."""
import pandas as pd


regex_twitter = r"(http(?:s)?:\/\/(?:www\.)?twitter\.com\/[a-zA-Z0-9_]+/status/[0-9]+)(\?s=\d+|/photo/\d+)?"
regex_facebook = (
    r"(http(?:s)?:\/\/(?:(?:www|m)\.)?)(facebook\.com\/[a-zA-Z0-9_\.]+\/(?:photos|posts|videos|)\/[0-9\/\.pcba]+)"
    r"((?:\?|__tn__).+)?"
)


def clean_urls(df: pd.DataFrame) -> pd.DataFrame:
    # Twitter
    msk = df.source_url.str.match(regex_twitter)
    df.loc[msk, "source_url"] = df.loc[msk, "source_url"].str.extract(regex_twitter)[0]

    # Facebook
    msk = df.source_url.str.fullmatch(regex_facebook)
    df.loc[msk, "source_url"] = (
        "https://www." + df.loc[msk, "source_url"].str.extract(regex_facebook)[1]
    )

    return df
