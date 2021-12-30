from .base_writer import BaseWriter


class ParquetWriter(BaseWriter):
    """Writer class for writing koalas.DataFrame to PARQUET file"""

    def write(self, kdf, path: str, partition_cols=None) -> None:
        """Writes koalas.DataFrame to PARQUET file with option for partitioning
        :param kdf: koalas.DataFrame
        :param path: path to file
        :param partition_cols: columns to partition file
        :return: None
        """
        self.logger.info(f'Writing kdf as PARQUET file to {path}')
        kdf.to_parquet(path=path, partition_cols=partition_cols)
