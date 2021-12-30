import databricks.koalas as ks
from .base_reader import BaseReader


class JsonToKoalasReader(BaseReader):
    """Reader class for reading json datasets to koalas.Dataframe"""

    def read(self, path: str) -> ks.DataFrame:
        """Reads path to json dataset file and returns koalas.DataFrame
        :param path: path to file
        :return: koalas.DataFrame
        """
        self.logger.info(f'Reading "{path}" to kdf')
        return ks.read_json(path=path)
