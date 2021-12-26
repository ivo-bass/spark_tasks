from pyspark.sql import DataFrame, SparkSession
from .base_reader import BaseReader


class JsonToSparkReader(BaseReader):
    def __init__(self, logger, spark: SparkSession, path: str):
        super().__init__(logger, path)
        self.spark = spark

    def read(self) -> DataFrame:
        self.logger.info(f'Reading "{self.path}" to sdf')
        return self.spark.read.json(path=self.path)
