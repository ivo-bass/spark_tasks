from .base_writer import BaseWriter


class CsvWriter(BaseWriter):
    """Writer class for writing koalas.DataFrame to CSV file"""

    def write(self, kdf, path: str, partition_cols=None) -> None:
        """Writes koalas.DataFrame to CSV file with option for partitioning
        :param kdf: koalas.DataFrame
        :param path: path to file
        :param partition_cols: columns to partition file
        :return: None
        """
        self.logger.info(f'Writing kdf to {path} as CSV file')
        kdf.to_csv(path=path, partition_cols=partition_cols)
