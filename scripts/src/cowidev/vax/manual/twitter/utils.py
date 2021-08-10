import tweepy


class TwitterAPI:
    def __init__(self, consumer_key: str, consumer_secret: str):
        self._api = self._get_api(consumer_key, consumer_secret)

    def _get_api(self, consumer_key, consumer_secret):
        auth = tweepy.AppAuthHandler(consumer_key, consumer_secret)
        return tweepy.API(auth)

    def get_tweets(self, username, num_tweets=100):
        tweets = self._api.user_timeline(
            screen_name=username,
            # 200 is the maximum allowed count
            count=num_tweets,
            include_rts=False,
            # Necessary to keep full_text
            # otherwise only the first 140 words are extracted
            tweet_mode="extended",
            exclude_replies=True,
        )
        return tweets
