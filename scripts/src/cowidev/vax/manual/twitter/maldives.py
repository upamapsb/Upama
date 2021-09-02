import pandas as pd
import re

from cowidev.utils.clean import clean_date
from cowidev.vax.manual.twitter.base import TwitterCollectorBase


class Maldives(TwitterCollectorBase):
    def __init__(self, api, paths=None):
        super().__init__(
            api=api,
            username="HPA_MV",
            location="Maldives",
            add_metrics_nan=True,
            paths=paths,
        )

    def _propose_df(self):
        regex = r"COVID-19 : Vaccination Updates\n\n(\d{1,2}\.\d{1,2}\.202\d).*"
        data = []
        for tweet in self.tweets:
            match = re.search(regex, tweet.full_text)
            if match:
                dt = clean_date(match.group(1), "%d.%m.%Y")
                if self.stop_search(dt):
                    break
                data.append(
                    {
                        "date": dt,
                        "text": tweet.full_text,
                        "source_url": self.build_post_url(tweet.id),
                        "media_url": tweet.entities["media"][0]["media_url_https"]
                        if "media" in tweet.entities
                        else None,
                    }
                )
        return pd.DataFrame(data)


def main(api, paths):
    Maldives(api, paths).to_csv()
