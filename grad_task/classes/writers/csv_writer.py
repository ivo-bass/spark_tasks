from .base_writer import BaseWriter


class CsvWriter(BaseWriter):
    def write(self, kdf, path, partition_cols=None) -> None:
        self.logger.info(f'Writing kdf to {path} as CSV file')
        kdf.to_csv(path=path, partition_cols=partition_cols)
