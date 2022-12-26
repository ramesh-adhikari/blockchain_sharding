from enum import Enum


class State(Enum):
    NONE = 0
    PREPARING = 1
    COMMITING = 2
    ABORTING = 3
