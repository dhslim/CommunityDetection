from datetime import datetime
from util import LocalSearchMap
from database_handler import DatabaseHandler
from tweepy_handler import TweepyHandler
from typing import List, Tuple
from collections import OrderedDict
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import AffinityPropagation

class Ranking:
    """Class hosting the ranking functions."""

    def __init__(self, start_date: datetime, end_date: datetime):
        """Creates a new Ranking class instance based on a particular timeframe."""
        self.start_date = start_date
        self.end_date = end_date
        self.th = TweepyHandler()
        self.db = DatabaseHandler()

    def local_nbhd(self, agent: str, community: str) -> List[str]:
        """Return a list of screen names of users who are in the local neighbourhood of the 'agent'.
        'agent' will be the first screen name in the list."""
        local = [friend for friend in self.th.get_friends(agent)
                 if self.db.user_in_comm(friend, community, self.start_date, self.end_date)]
        return [agent] + local

    def user_friends(self, agent):
        local = [friend for friend in self.th.get_friends(agent)]
        return [agent] + local

    def search_by_consumption(self, seed: str, community: str) -> List[str]:
        """Local search algorithm that tries to find the core of a community through consumption utility, starting at
        the seed user."""
        return self._search_by_utility('consumption', seed, community)

    def search_by_production(self, seed: str, community: str) -> List[str]:
        """Local search algorithm that tries to find the core of a community through production utility, starting at
        the seed user."""
        return self._search_by_utility('production', seed, community)

    def _search_by_utility(self, utility_type: str, seed: str, community: str) -> List[str]:
        """Helper method for finding the core of a community based on utility type."""
        # Load community consumption map from database into LSM
        db_map = self.db.get_label_graph(community, utility_type, self.start_date, self.end_date)
        ls_map = LocalSearchMap(db_map['map'], community, utility_type, self.start_date, self.end_date)
        path_taken = [seed]
        curr = seed

        # Full path is in db.
        if ls_map.contains_node(curr):
            return ls_map.generate_path_from(curr)
        local = self.local_nbhd(curr, community)
        local_max = self.get_max_utility(utility_type, local)

        while curr != local_max:
            # Add node to path
            print('Local Max of', curr, ':', local_max)
            path_taken.append(local_max)
            curr = local_max
            # Part of the path is in db.
            if ls_map.contains_node(curr):
                self.db.add_path_to_label_graph(path_taken, community, utility_type, self.start_date, self.end_date)
                return path_taken[:-1] + ls_map.generate_path_from(curr)
            local = self.local_nbhd(curr, community)
            local_max = self.get_max_utility(utility_type, local)

        # No part of the path is in db.
        self.db.add_path_to_label_graph(path_taken, community, utility_type, self.start_date, self.end_date)
        return path_taken

    def get_max_utility(self, util_type: str, local: List[str]) -> str:
        """Return the agent with highest utility of 'util_type' in the given local neighborhood."""
        if util_type == 'consumption':
            return self.get_max_consumption(local)
        else:
            assert util_type == 'production'
            return self.get_max_production(local)

    def get_max_consumption(self, local: List[str]) -> str:
        """Helper function that returns the agent with highest consumption utility in the given local neighborhood.
        In the case of a tie, the user that was checked first is chosen.
        """
        highest_consumer = ''
        highest_consumption = -1

        for user in local:
            retweets, _ = self.db.get_tweets(user, self.start_date, self.end_date)
            score = self.consumption_score(user, retweets, local)
            # TODO ditto
            # print("just checked user", user, "score", score)
            # what if there is a tie? its fine cuz we have [agent] + local
            if score > highest_consumption:
                highest_consumer = user
                highest_consumption = score

        return highest_consumer

    def get_n_consumption(self, local, n):
        scores = OrderedDict.fromkeys(local, 0)
        for user in local:
            retweets, _ = self.db.get_tweets(user, self.start_date, self.end_date)
            score = self.consumption_score(user, retweets, local)
            scores[user] = score
        print(scores)
        return sorted(scores, key=scores.get, reverse=True)[:n]

    def consumption_score(self, user: str, retweets: List[Tuple[str, str]], local: List[str]) -> int:
        """Helper function to consumption ranking function. Gives the consumption utility score of 'user' by their
        retweets and local neighborhood.
        """
        score = 0
        for retweet in retweets:
            if retweet[1] != user and retweet[1] in local:
                score += 1

        return score

    def get_max_production(self, local: List[str]) -> str:
        """Helper function that returns the user with highest production utility in the given local neighborhood."""
        # OrderedDict ensures the 'agent' of local nbhd is priotized in the case of a tie.
        scores = self.get_production_scores(local)
        # print(score)
        return max(scores, key=scores.get)

    def get_n_production(self, local: List[str], n) -> List[str]:
        """Helper function that returns the user with highest production utility in the given local neighborhood."""
        # OrderedDict ensures the 'agent' of local nbhd is priotized in the case of a tie.
        scores = self.get_production_scores(local)
        print(scores)
        return sorted(scores, key=scores.get, reverse=True)[:n]

    def get_production_scores(self, local):
        score = OrderedDict.fromkeys(local, 0)
        for user in local:
            retweets, _ = self.db.get_tweets(user, self.start_date, self.end_date)
            for retweet in retweets:
                if retweet[1] != user and retweet[1] in local:
                    score[retweet[1]] += 1
                    # print(retweet, '\nog:', retweet[1], 'rtwer:', user)
        return score

    def get_corpus(self, local):
        corpus = []
        for user in local:
            corpus.append(self.db.timeline_to_document(user, self.start_date, self.end_date))
        return corpus

    def tfidf_vectorizer(self, local):
        corpus = self.get_corpus(local)
        vectorizer = TfidfVectorizer()
        return vectorizer, vectorizer.fit_transform(corpus)

    def timeline_to_document(self, user):
        return self.db.timeline_to_document(user, self.start_date, self.end_date)



if __name__ == '__main__':
    r = Ranking(datetime(2019, 3, 1), datetime(2019, 5, 1))
    # print(r.db.timeline_to_document('david_madras', datetime(2019, 3, 1), datetime(2019, 5, 1)))
    local = r.user_friends('jonLorraine9')
    with open("jonLorraine9.txt", "r") as myfile:
        data = myfile.read()
    # print(data)

    for user in local:
        if user not in data:
            print(user)
    # print(r.get_corpus(local))

    # for user in local:
    #     print(r.th.api.get_user(user).id)
    # r.th.api.

    # vectorizer, X = r.tfidf_vectorizer(local)
    # print(vectorizer.get_feature_names())
    # print(X)
    # print(type(X))
    # print(X.shape)
    # clustering = AffinityPropagation().fit(X)
    # print(clustering.cluster_centers_)
    # print(clustering)
    # print(11111111)
    # # print(X[0, :].nonzero())
    # print(clustering.labels_)
    # dict ={}
    # for i in range(0, len(clustering.labels_)):
    #     if clustering.labels_[i] not in dict:
    #         dict[clustering.labels_[i]] = [local[i]]
    #     else:
    #         dict[clustering.labels_[i]].append(local[i])
    # print(dict)
    # print(22222)
    # print(clustering.cluster_centers_indices_)
    # for user_i in clustering.cluster_centers_indices_:
    #     print(local[user_i])
    # for index in clustering.cluster_centers_indices_:
    #     print(index)
    #     print([vectorizer.get_feature_names()[i] for i in X[index, :].nonzero()[1]])





    ###
    # local = r.local_nbhd('david_madras', 'machine learning')
    # print('Prod: \n', r.get_n_production(local, 20))
    # print('Cons: \n', r.get_n_consumption(local, 20))
