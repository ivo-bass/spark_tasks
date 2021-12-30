import databricks.koalas as ks
from pyspark.sql import DataFrame


class KdfBuilder:
    """Builds koalas.DataFrame"""

    @staticmethod
    def build(columns: list, path: str = None, sdf: DataFrame = None, reader=None) -> ks.DataFrame:
        """Builds koalas.DataFrame from spark.DataFrame.
        If spark.DataFrame is not passed, reader should be passed
        to read dataset and build spark.DataFrame.
        :param columns: list of columns to select from sdf
        :param path: path to file
        :param sdf: spark DataFrame
        :param reader: Reader() to read dataset -> sdf
        :return: databricks.koalas.DataFrame
        """
        if reader and not sdf:
            sdf = reader.read(path=path)
        sdf_columns = sdf.select(columns)
        return ks.DataFrame(data=sdf_columns)
