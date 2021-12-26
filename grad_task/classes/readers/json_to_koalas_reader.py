import databricks.koalas as ks
from .base_reader import BaseReader


class JsonToKoalasReader(BaseReader):
    def read(self) -> ks.DataFrame:
        self.logger.info(f'Reading "{self.path}" to kdf')
        return ks.read_json(path=self.path)
