from abc import ABC, abstractmethod


class BaseReader(ABC):
    def __init__(self, logger) -> None:
        self.logger = logger

    @abstractmethod
    def read(self, path: str):
        raise NotImplementedError('Reading should be implemented in a subclass')
