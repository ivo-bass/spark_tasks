from pyspark.sql import DataFrame, SparkSession
from .base_reader import BaseReader


class JsonToSparkReader(BaseReader):
    """Reader class for reading json datasets to spark.Dataframe"""

    def __init__(self, logger, spark: SparkSession) -> None:
        super().__init__(logger)
        self.spark = spark

    def read(self, path: str) -> DataFrame:
        """Reads path to json dataset file and returns spark.DataFrame
        :param path: path to file
        :return: spark.DataFrame
        """
        self.logger.info(f'Reading "{path}" to sdf')
        return self.spark.read.json(path=path)
