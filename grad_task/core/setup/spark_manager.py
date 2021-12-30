from pyspark.sql import SparkSession


class SparkManager:
    """This class holds SparkSession configurations and starts spark app"""
    def __init__(self, spark_config: dict) -> None:
        self.app_name = spark_config['appname']
        self.master = spark_config['master']

    def start_spark(self, logger) -> SparkSession:
        """Instantiates SparkSession with configurations
        :return: SparkSession
        """
        logger.info(f'___Starting SPARK application___')
        spark = SparkSession.builder \
            .appName(self.app_name) \
            .master(self.master) \
            .getOrCreate()
        spark.conf.set('spark.sql.execution.arrow.pyspark.enabled', "true")
        return spark
