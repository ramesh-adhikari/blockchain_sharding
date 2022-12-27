from enum import Enum


class State(Enum):
    NONE = 0
    PREPARING = 1  # leader picked transaction, splitted to subtransaction, send commands to destination shards
    COMMITING = 2  # leder received commit votes from all destination shards, leader sends commit command to destination
    ABORTING = 3
