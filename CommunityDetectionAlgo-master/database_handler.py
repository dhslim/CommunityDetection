import pymongo
import tweepy_handler
import keys
import re
import ssl
from datetime import datetime
from typing import List, Tuple, Union, Dict
from tweepy.error import TweepError
import nltk
from collections import Counter



class DatabaseHandler:
    """Wrapper class for the get_tweets function and get_keywords."""

    def __init__(self):
        """Initializes a new DatabaseHandler."""
        self.client = pymongo.MongoClient(keys.mongo_key, ssl=True, ssl_cert_reqs=ssl.CERT_NONE)
        self.th = tweepy_handler.TweepyHandler()

    def get_keywords(self, community: str) -> List[str]:
        """Returns a list of all keywords belonging to a given community.
        Notifies user if such a community does not exist.
        """
        keywords_col = self.client['productionFunction']['keywords']
        try:
            result = keywords_col.find_one({"name": community})
            return result['tags']
        except TypeError:
            print("community was not found")

    def get_tweets(self, user: Union[str, int], start: datetime, end: datetime) \
            -> Tuple[List[Tuple[str, str]], List[str]]:
        """Returns the full doc of Retweets (with the author of the original Tweet) and Tweets of a user
        within a timeframe.
        If this data is not logged in the mongoDB yet, it gathers this data from tweepy; if already logged,
        it takes this data from the mongoDB.
        Users with suspended/deleted accounts will be logged in the mongoDB with their id value as -1.
        """
        # First check if mongodb has what we need.
        users_col = self.client['productionFunction']['users']
        query = {"$or": [{"handle": user}, {"id": user}], "start": start, "end": end}
        user_doc = users_col.find_one(query)
        if user_doc is None:
            # Make a tweepy call if the database doesn't have this info yet.
            retweets, tweets = self.th.get_tweets(user, start, end)
            try:
                user_data = self.th.api.get_user(user)
                d = {"handle": user_data.screen_name, "id": user_data.id, "start": start, "end": end,
                     "retweets": retweets,
                     "tweets": tweets}
            except TweepError:
                print(user, "does not exist")
                d = {"handle": user, "id": -1, "start": start, "end": end, "retweets": retweets, "tweets": tweets}
            users_col.insert_one(d)
            return retweets, tweets
        else:
            if user_doc['id'] == -1:
                print(user, "does not exist")
            return user_doc['retweets'], user_doc['tweets']

    def timeline_to_document(self, user: Union[str, int], start: datetime, end: datetime) -> str:
        """Returns the concatentation of all the tweets/retweets as one string."""
        doc = ""
        tweets = self.get_tweets(user, start, end)
        # print(tweets)
        for retweet in tweets[0]:
            doc += retweet[0] + " "
        for tweet in tweets[1]:
            doc += tweet + " "

        # print(doc)
        doc = re.sub(r"\bhttps:\S*\b", "", doc)
        doc = re.sub(r"\b\d*\b", "", doc)
        #leave handle
        doc = re.sub(r"[^\w\s@]", "", doc)
        return doc
        # return text_to_words(doc)

    def user_in_comm(self, user: Union[str, int], community: str, start_date: datetime, end_date: datetime,
                     interest_threshold: int = 0.05) -> bool:
        """Returns True if a user is considered a part of the community based on their tweets within a timeframe and
        the interest threshold. Returns False otherwise.
        """
        keywords = self.get_keywords(community)
        # TODO remove from final product
        print("currently checking:", user)
        retweets, tweets = self.get_tweets(user, start_date, end_date)
        all_tweets = [retweet[0] for retweet in retweets] + tweets
        int_twts = 0
        if len(all_tweets) == 0:
            return False
        for tweet in all_tweets:
            for keyword in keywords:
                # A bit diff from sarah's regex but close enough
                re_keyword = r'(?i)\b' + keyword + r's?\b'
                if re.search(re_keyword, tweet) is not None:
                    print('key:', keyword, 'tweet:', tweet)
                    int_twts += 1
                    break
        print(int_twts, len(all_tweets), int_twts/len(all_tweets))
        return int_twts / len(all_tweets) >= interest_threshold

    def get_label_graph(self, community: str, ranking_type: str, start, end) -> Dict:
        """Gets the label graph of a given community stored in MongoDB for consumption/production utility.
        """
        graph_col = self.client['productionFunction']['labelGraphs']
        d = graph_col.find_one_and_update({'community': community, 'ranking_type': ranking_type,
                                           'timeframe': (start, end), 'map': {'$exists': True}},
                                          {'$setOnInsert': {'map': {}}},
                                          upsert=True, return_document=True)
        print(d)
        return d

    def add_path_to_label_graph(self, path: List[str], community: str, ranking_type: str, start: datetime,
                                end: datetime) -> None:
        """Add path to label graph stored in MongoDB. Create a label graph if it doesn't exist."""
        collection = self.client['productionFunction']['labelGraphs']
        for i in range(0, len(path)):
            if i < len(path) - 1:
                collection.update_one({'community': community, 'ranking_type': ranking_type,
                                       'timeframe': (start, end)
                                       },
                                      {'$set': {'map.' + path[i]: path[i + 1]}
                                       }, upsert=True)
            else:
                collection.update_one({'community': community, 'ranking_type': ranking_type,
                                       'timeframe': (start, end)
                                       },
                                      {'$set': {'map.' + path[i]: path[i]}
                                       }, upsert=True)

    def get_global_freq(self):
        # should store raw data
        global_data = self.client['globalData']['wordCount']
        global_dict = global_data.find_one()
        global_data2 = self.client['globalData']['wordCount1']
        global_dict2 = global_data2.find_one()
        del global_dict['_id']
        del global_dict2['_id']
        return Counter(global_dict) + Counter(global_dict2)

    def word_to_global_count(self, word: str) -> None:
        global_count = self.client['globalData']['wordCount1']
        global_dict = global_count.find_one()
        if global_dict is None:
            global_count.insert_one({})
        global_count.update_one({}, {'$inc': {word: 1}})


def text_to_words(text):
    words = re.sub(r"\bhttps:\S*\b", "", text.lower())
    words = re.sub(r"\b\d*\b", "", words)
    list_words = re.sub(r"[^\w]", " ", words).split()
    return list_words




if __name__ == "__main__":
    d = DatabaseHandler()
    # d.user_in_comm('MrsCaroline_C', 'machine learning', datetime(2019,3,1), datetime(2019,5,1))
    # print(d.timeline_to_document('david_madras', datetime(2019,4,25), datetime(2019,5,1)))
    # d.word_to_global_count('hi')
    from word_freq import BatchCount
    # print(BatchCount(d.get_global_freq()))
    global_data = d.client['globalData']['wordCount']
    global_dict = global_data.find_one()
    global_data2 = d.client['globalData']['wordCount1']
    global_dict2 = global_data2.find_one()
    del global_dict['_id']
    del global_dict2['_id']
    print(BatchCount(Counter(global_dict)))
    print(BatchCount(Counter(global_dict2)))
