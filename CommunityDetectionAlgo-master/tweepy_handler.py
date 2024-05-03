import keys
import tweepy
from datetime import datetime
from typing import List, Tuple, Union, Generator
import itertools
import random


class TweepyHandler:
    """Wrapper class for dealing with the Twitter API using tweepy."""

    def __init__(self) -> None:
        """Creates a new TweepyHandler."""
        auth = tweepy.OAuthHandler(keys.consumer_key, keys.consumer_secret_key)
        auth.set_access_token(keys.access_token, keys.access_token_secret)
        self.api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

    def get_tweets(self, user: Union[int, str], start: datetime, end: datetime) \
            -> Tuple[List[Tuple[str, str]], List[str]]:
        """Return the full doc of Retweets (with the author of the original Tweet) and Tweets of a user
        within a timeframe.
        User's with private/suspended/deleted accounts will return empty Tweets and Retweets.
        """
        statuses = tweepy.Cursor(self.api.user_timeline, id=user, tweet_mode='extended', count=200).items()
        retweets = []
        tweets = []
        try:
            for status in statuses:
                if start <= status.created_at < end and 'retweeted_status' in status._json:
                    # retweet = (full doc of rwt, original author)
                    # full_text vs doc? TODO
                    retweet = status._json['retweeted_status']['full_text'], \
                              status._json['retweeted_status']['user']['screen_name']
                    retweets.append(retweet)
                elif start <= status.created_at < end and 'retweeted_status' not in status._json:
                    tweet = status._json['full_text']
                    tweets.append(tweet)
                elif start > status.created_at:
                    break
        # TweepError is raised when the user's account is either private, suspended, or deleted.
        except tweepy.error.TweepError:
            print('TweepError on ', user)
        return retweets, tweets

    def get_friends(self, user: str) -> Generator[str, None, None]:
        """Return a list of all the screen names of users that 'user' follows."""
        # This is where a lot of the rate limting occurs 2 diff implementation for small and big?
        friends = tweepy.Cursor(self.api.friends_ids, screen_name=user, count=5000).items()

        def grouper(n, iterator):
            while True:
                chunk = list(itertools.islice(iterator, n))
                if not chunk:
                    return
                yield chunk
        # make this a generator instead of a list
        for hund_ids in grouper(100, friends):
            for user in self.api.lookup_users(hund_ids):
                yield user.screen_name

    def rate_limit(self) -> None:
        """Print remaining API calls to functions that do not have full API calls."""
        rate_data = self.api.rate_limit_status()
        for res in rate_data['resources'].keys():
            for func in rate_data['resources'][res].keys():
                if rate_data['resources'][res][func]['limit'] != rate_data['resources'][res][func]['remaining']:
                    print(func, rate_data['resources'][res][func]['remaining'],
                          rate_data['resources'][res][func]['limit'])
    def random_tweets(self):
        return self.myStream.sample()

if __name__ == "__main__":
    t = TweepyHandler()
    for friend in t.get_friends('jonLorraine9'):
        print(friend)
        t.get_tweets(friend, datetime(2019,1,14), datetime(2019,7,14))
        print('completed')
    t.get_tweets('jonLorraine9', datetime(2019, 1, 14), datetime(2019, 7, 14))

    # while True:
    #     try:
    #         id = random.randint(10**0, 10**9)
    #         print(id, len(str(id)))
    #         # print(t.api.get_user(user_id=id))
    #         print(t.api.get_status(id=id))
    #     except tweepy.TweepError:
    #         continue
    # l = 0
    # print(t.random_tweets())
    # for s in t.api.user_timeline(id='hardmaru', tweet_mode='extended', count=200):
    #     # print(s)
    #     l += 1
    # print(l)
    # l = [s for s in t.api.user_timeline(id='hardmaru', tweet_mode='extended', count=200)]
    # len(l)
    # tw = t.get_tweets('hardmaru', datetime(2019,5,1), datetime(2019,6,25))
    # t.rate_limit()
    # print(tw)
    # # print(len(tw))
    # print(len(tw[0]) + len(tw[1]))
    # t.rate_limit()
