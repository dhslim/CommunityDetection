import json
import os

from word_freq import WordFreq

print(os.getcwd())
tweets_data_path = './6_28_1730.txt'

tweets_data = []
tweets_file = open(tweets_data_path, "r")
for line in tweets_file:
    # print(line)
    # print(type(line))
    tweet = json.loads(line)
    tweets_data.append(tweet)

print(len(tweets_data))

w = WordFreq.word_count()


# make sure to check the data im getitng is not corrupt ie no repetition, english, full doc
