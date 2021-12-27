from pyspark.sql import DataFrame, SparkSession
from .base_reader import BaseReader


class JsonToSparkReader(BaseReader):
    def __init__(self, logger, spark: SparkSession):
        super().__init__(logger)
        self.spark = spark

    def read(self, path: str) -> DataFrame:
        self.logger.info(f'Reading "{path}" to sdf')
        return self.spark.read.json(path=path)
