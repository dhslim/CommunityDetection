import json
import re
import nltk
from database_handler import DatabaseHandler
from typing import List

class Process:
    def __init__(self):
        self.filepaths = []
        self.db = DatabaseHandler()

    def process_file(self, filespaths: List[str]):
        self.filepaths += filespaths
        for text in self.read_line():
            for word in self.process_text(text):
                self.db.word_to_global_count(word)

    def read_line(self):
        for filepath in self.filepaths:
            file = open(filepath, "r")
            for line in file:
                try:
                    tweet = json.loads(line)
                    if 'extended_tweet' in tweet:
                        text = tweet['extended_tweet']['full_text']
                    elif 'retweeted_status' in tweet:
                        text = tweet['retweeted_status']['text']
                    else:
                        text = tweet['text']
                except json.decoder.JSONDecodeError:
                    continue
                except TypeError:
                    continue
                yield text

    def save_to_db(self, list_words: List[str]):
        for word in list_words:
            self.db.word_to_global_count(word)


    def process_text(self, text: str) -> List[str]:
        text = re.sub(r"\bhttps:\S*\b", "", text)
        text = re.sub(r"\b\d*\b", "", text)
        # @ for handles? [^\w\s@]
        text = re.sub(r"[^\w\s@]", "", text)
        text = text.lower()
        text = text.split()
        sno = nltk.stem.SnowballStemmer('english')
        return [sno.stem(x) for x in text]

if __name__ == '__main__':
    import sys
    p = Process()

    filepath = sys.argv[1:]
    p.process_file(filepath)
# if __name__ == "__main__":
#     p = Process()
#     # print(p)
#     # p.filepaths = ['./6_28_1810.txt']
#     # # print(p.filepaths)
#     # for line in p.read_line():
#     #     print(p.process_text(line))
#     p.process_file(['6_28_1810.txt'])

# text_file_name = '6_30_0741'
# tweets_data_path = f'./{text_file_name}.txt'
#
# tweets_data = []
# tweets_file = open(tweets_data_path, "r")
# for line in tweets_file:
#     try:
#         tweet = json.loads(line)
#         if 'extended_tweet' in tweet:
#             text = tweet['extended_tweet']['full_text']
#         elif 'retweeted_status' in tweet:
#             text = tweet['retweeted_status']['text']
#         else:
#             text = tweet['text']
#     except json.decoder.JSONDecodeError:
#         continue
#     except TypeError:
#         continue
#     assert isinstance(text, str)
#
#     tweets_data.append(text)
#
# doc = ' '.join(tweets_data)
# doc = re.sub(r"\bhttps:\S*\b", "", doc)
# doc = re.sub(r"\b\d*\b", "", doc)
# #TODO!!! @ for handles? [^\w\s@]
# doc = re.sub(r"[^\w\s]", "", doc)
# doc = doc.lower()
# sno = nltk.stem.SnowballStemmer('english')
# s = doc.split()
# d = pd.DataFrame(s)
# s1 = d[0].apply(lambda x: sno.stem(x))
# counts = Counter(s1)
# # print(counts)
# # print(tweets_data)
# #
# # print(len(tweets_data))
#
# d = DatabaseHandler()
# client = d.client
# db = client['globalData']
# collection = db['wordCount']
# # print(collection.find())
# doc = collection.find_one()
# counter = Counter(doc)
# del counter['_id']
# # print(counter)
# # print(counts)
# new_counter = counter + counts
# stopwords = set(nltk.corpus.stopwords.words('english'))
# stopwords.add('amp')
# for word in stopwords:
#     if word in new_counter:
#         del new_counter[word]
#
# list_of_txt = db['txtFiles']
# list_of_txt.insert_one({text_file_name: 1})
#
#
# if collection.find_one() is None:
#     collection.insert_one(new_counter)
# else:
#     collection.replace_one({}, new_counter)
