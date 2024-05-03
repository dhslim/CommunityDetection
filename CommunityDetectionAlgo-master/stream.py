import tweepy
import keys
import pymongo
import ssl


class MyStreamListener(tweepy.StreamListener):
    def __init__(self):
        super().__init__()
        self.statuses = []
        self.counter = 0
        self.save_file = open('tweets.json', 'w')


    def on_data(self, data):
        print(data)

    def save_to_db(self, status):
        client = pymongo.MongoClient(keys.mongo_key, ssl=True, ssl_cert_reqs=ssl.CERT_NONE)
        if 'retweeted_status' in status._json:
            text = status._json['retweeted_status']['doc']
        else:
            try:
                text = status._json['extended_tweet']['full_text']
            except KeyError:
                text = status._json['doc']
        col = client['globalTweets']['randomStream']
        col.insert_one({'doc': text})

class MyStream():
    def __init__(self) -> None:
        """Creates a new myStream."""
        auth = tweepy.OAuthHandler(keys.consumer_key, keys.consumer_secret_key)
        auth.set_access_token(keys.access_token, keys.access_token_secret)
        api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
        my_stream_listener = MyStreamListener()
        self.stream = tweepy.Stream(auth=api.auth, listener=my_stream_listener)

if __name__ == "__main__":
    my_stream = MyStream()
    my_stream.stream.sample(languages=["en"])
