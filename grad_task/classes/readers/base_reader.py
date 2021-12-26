from abc import ABC, abstractmethod


class BaseReader(ABC):
    def __init__(self, logger, path: str) -> None:
        self.logger = logger
        self.path = path

    @abstractmethod
    def read(self):
        raise NotImplementedError('Reading should be implemented in a subclass')
