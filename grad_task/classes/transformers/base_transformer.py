from abc import ABC


class BaseTransformer(ABC):
    def __init__(self, logger):
        self.logger = logger

