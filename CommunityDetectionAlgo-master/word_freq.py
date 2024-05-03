from collections import Counter
import pandas as pd
import nltk
from local_search import *
import numpy as np
from wordfreq import word_frequency
import copy


class BatchCount:

    def __init__(self, counter: Counter, total: int=None) -> None:
        self.counter = counter
        self.total = total if total else sum(counter.values())

    def __add__(self, other: 'BatchCount') -> 'BatchCount':
        """Return added"""
        return BatchCount(self.counter + other.counter, self.total + other.total)

    def __str__(self):
        return f'BatchCount(\nCounter:{self.counter}\nTotal:{self.total})'

    def update(self, other: 'BatchCount') -> None:
        """Modify by adding"""
        self.counter.update(other.counter)
        self.total += other.total

    def remove_word(self, word: str) -> None:
        try:
            self.total -= self.counter[word]
            del self.counter[word]
        except KeyError:
            print(f'{word} not in BatchCount')


# extract useful class/interface out of ranking since it is reused here
class WordFreq:

    def __init__(self, start, end):
        self.r = Ranking(start, end)

    def friends(self, user: str) -> List[str]:
        """Return the list of friends of a user."""
        return self.r.user_friends(user)

    def word_count(self, text: str) -> Counter:
        """Return the word count of given text."""
        if not text:
            return Counter()
        text = text.lower()
        sno = nltk.stem.SnowballStemmer('english')
        s = text.split()
        d = pd.DataFrame(s)
        s1 = d[0].apply(lambda x: sno.stem(x))
        counts = Counter(s1)
        return counts

    def user_word_total(self, user):
        return sum(self.user_count(user).values())

    def user_count(self, user: str) -> Counter:
        """Return the word count of a user."""
        user_text = self.r.timeline_to_document(user)
        return self.word_count(user_text)

    def local_count(self, local) -> BatchCount:
        """Return the local word count."""
        local_count = BatchCount(Counter())
        for user in local:
            user_count = BatchCount(self.user_count(user))
            local_count.update(user_count)
        return local_count

# change this percentage 0.05
    def word_dist(self, local) -> Counter:
        """Return a counter of how many users in the local neighbourhood(value) used the word(key)."""
        word_dist = Counter()
        for user in local:
            user_count = self.user_count(user)
            for word in user_count.keys():
                if word_dist[word]:
                    word_dist[word] += 1
                else:
                    word_dist[word] = 1
        return word_dist

# I need to run stemming
    def processed_local_count(self, local) -> BatchCount:
        """Return the processed local count."""
        local_count = self.local_count(local)
        # Remove stopwords.
        stopwords = set(nltk.corpus.stopwords.words('english'))
        stopwords.add('amp')
        for word in stopwords:
            if word in local_count.counter:
                #TODO extract
                local_count.remove_word(word)
        # # Remove words used by less than n users in the local neighbourhood.
        # n = 4
        # for word, count in self.word_dist(local).items():
        #     if count < n:
        #         local_count.remove_word(word)
        return local_count

    def global_count(self) -> BatchCount:
        """Return the real global count from the database."""
        return BatchCount(self.r.db.get_global_freq())

    def processed_global_count(self) -> BatchCount:
        """Return the processed global count."""
        # Remove stopwords
        global_count = self.global_count()
        stopwords = set(nltk.corpus.stopwords.words('english'))
        stopwords.add('amp')
        for word in stopwords:
            if word in global_count.counter:
                global_count.remove_word(word)
        return global_count

    def local_plus_global_count(self, local) -> BatchCount:
        """Return the local + global word count."""
        return self.processed_local_count(local) + self.processed_global_count()

    def processed_local_plus_global_count(self, local) -> BatchCount:
        """Return the processed local + global word count."""
        unprocessed_count = self.local_plus_global_count(local)
        processed_count = copy.deepcopy(unprocessed_count)
        # Total count can't be less than n
        n = 4
        for word, count in unprocessed_count.counter.items():
            if count < n:
                processed_count.remove_word(word)
        # Remove words used by less than n users in the local neighbourhood.
        n = 4
        for word, count in self.word_dist(local).items():
            if count < n:
                processed_count.remove_word(word)
        return processed_count

    def relative_frequency(self, user: str, _global: BatchCount) -> Counter:
        """Calculate and return the relative frequency of a user given the global word count."""
        user_count = self.user_count(user)
        rel_freq = copy.copy(_global.counter)
        for word, count in rel_freq.items():
            if word in user_count:
                rel_freq[word] = user_count[word]/count
            else:
                rel_freq[word] = 0
        return rel_freq

    def words_high_freq(self, local: List[str], _global: BatchCount, n=20) -> Counter:
        counter = Counter()
        for user in local:
            rel_freq = self.relative_frequency(user, _global)
            for k, v in rel_freq.most_common(n):
                if k not in counter:
                    counter[k] = 1
                else:
                    counter[k] += 1
        return counter

if __name__ == "__main__":
    w = WordFreq(datetime(2019, 3, 1), datetime(2019, 5, 1))
    local = w.friends('jonLorraine9')
    local = [user for user in local if w.user_word_total(user) > 20]
    # print(w.processed_local_count(local))
    # print(w.global_count())
    # print(w.processed_global_count())
    # print(w.local_plus_global_count(local))
    global_count = w.processed_local_plus_global_count(local)
    print(w.words_high_freq(local, global_count))
    # print(global_count)
    # # users = ['hardmaru', 'poolio', 'OpenAI', 'dpkingma', 'pycharm', 'BitMEXResearch', 'CBOE', 'NSERC_CRSNG', 'UofT',
    # #          'cityoftoronto']
    # d = {}
    # for user in local:
    #     print(f'USER:{user}')
    #     print(w.relative_frequency(user, global_count).most_common(20))
    #     d[user] = w.relative_frequency(user, global_count).most_common(20)
    #
    # s = {}
    # for user in d:
    #     words = set([k for k, v in d[user]])
    #     s[user] = words
    #
    # k = {}
    # for user in local:
    #     k[user] = []
    #     for user2 in s:
    #         if len(s[user] & s[user2]) > 3 and user != user2:
    #             k[user].append(user2)
    #             print(user, user2)
    #             print(s[user] & s[user2])
    # print(k)
