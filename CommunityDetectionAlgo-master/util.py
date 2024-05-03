from datetime import datetime
from typing import List, Dict


class LocalSearchMap:
    """A forest data structure representing paths to the core of a community. We implement this with a dictionary
    where each node in the forest and its parent is a key-value pair. In the context of production and consumption
    utility, a node is a user in the community and its parent is the user with the highest utility in its local
    neighbourhood. Note, multiple nodes can have the same parent. If a node is its own local max, it will be the root
    of a tree. So a path starts from a node and follows its ancestor until it reaches the root.
    """
    def __init__(self, map_dict: Dict[str, str], community: str, util_type: str, start_date: datetime,
                 end_date: datetime) -> None:
        """A LocalSearchMap."""
        self.map_dict = map_dict
        self.community = community
        if util_type in ['production', 'consumption']:
            self.util_type = util_type
        else:
            raise ValueError('Invalid utility type')
        self.start_date = start_date
        self.end_date = end_date

    def generate_path_from(self, start: str) -> List[str]:
        """Return a list of screen names of users in a path starting from 'start'."""
        path = [start]
        curr = start
        nxt = self.map_dict[start]
        while curr != nxt:
            path.append(nxt)
            curr = nxt
            nxt = self.map_dict[curr]
        return path

    def contains_node(self, user: str) -> bool:
        """Return True if 'user' is a node in LocalSearchMap, False otherwise."""
        return user in self.map_dict
