import databricks.koalas as ks
from .base_reader import BaseReader


class JsonToKoalasReader(BaseReader):
    def read(self, path) -> ks.DataFrame:
        self.logger.info(f'Reading "{path}" to kdf')
        return ks.read_json(path=path)
