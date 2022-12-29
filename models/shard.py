class Shard:
    def __init__(self, id: int, is_leader: bool) -> None:
        self.id = id
        self.is_leader = is_leader


#TODO check usage