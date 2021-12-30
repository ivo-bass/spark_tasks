import databricks.koalas as ks

from abc import ABC, abstractmethod


class BaseWriter(ABC):
    """Abstract class for writing DataFrames"""
    def __init__(self, logger) -> None:
        self.logger = logger

    @abstractmethod
    def write(self, kdf: ks.DataFrame, path: str, partition_cols=None) -> None:
        raise NotImplementedError('Writing should be implemented in a subclass')
