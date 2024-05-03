from collections import OrderedDict, Counter
import numpy
import scipy
from sklearn.cluster import AffinityPropagation
from word_freq import WordFreq
from typing import Dict
from sklearn.metrics.pairwise import cosine_distances

#Counters are ordered https://stackoverflow.com/questions/52174284/how-are-counter-defaultdict-ordered-in-python-3-7

class Cluster:

    vectors: Dict[str, Counter]

    def __init__(self):
        self.vectors = OrderedDict()

    def process_vectors(self, most=20):
        dim = set()
        for vector in self.vectors.values():
            for k, _ in vector.most_common(most):
                dim.add(k)
        for vector in self.vectors.values():
            for key in list(vector.keys()):
                if key not in dim:
                    del vector[key]

    def counter_to_array(self, counter: Counter) -> numpy.array:
        return numpy.array([x for x in counter.values()])

    def clustering(self):
        arrays = [self.counter_to_array(counter) for counter in self.vectors.values()]
        # print(arrays)
        return AffinityPropagation().fit(arrays)
    def clustering_cosine(self):
        word_cosine = cosine_distances([self.counter_to_array(counter) for counter in self.vectors.values()])
        print(word_cosine)
        affprop = AffinityPropagation(affinity='precomputed')
        return affprop.fit(word_cosine)

if __name__ == "__main__":
    from datetime import datetime
    c = Cluster()
    w = WordFreq(datetime(2019, 3, 1), datetime(2019, 5, 1))
    local = w.friends('jonLorraine9')
    _global = w.processed_local_plus_global_count(local)
    for user in local:
        c.vectors[user] = w.relative_frequency(user, _global)
    c.process_vectors()
    print(c.vectors)
    print([len(x) for x in c.vectors.values()])
    clustering = c.clustering_cosine()
    print(clustering.labels_)
    print(clustering.cluster_centers_)
    print(clustering.cluster_centers_indices_)
    d = {}
    for i in range(0, len(clustering.labels_)):
        if (clustering.labels_[i], local[clustering.cluster_centers_indices_[clustering.labels_[i]]]) in d:
            d[(clustering.labels_[i], local[clustering.cluster_centers_indices_[clustering.labels_[i]]])] += [local[i]]
        else:
            d[(clustering.labels_[i], local[clustering.cluster_centers_indices_[clustering.labels_[i]]])] = [local[i]]
    print(d)
