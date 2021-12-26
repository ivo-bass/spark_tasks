from .base_writer import BaseWriter


class ParquetWriter(BaseWriter):
    def write(self, kdf, path, partition_cols=None) -> None:
        self.logger.info(f'Writing kdf to {path} as PARQUET file')
        kdf.to_parquet(path=path, partition_cols=partition_cols)
