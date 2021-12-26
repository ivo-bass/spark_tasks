"""
Run script with:

spark-submit run.py config.yml >> info.log

"""
import argparse
import logging.config

import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


parser = argparse.ArgumentParser()
parser.add_argument('config', help='YAML config file')
args = parser.parse_args()
with open(args.config) as config_file:
    conf = yaml.safe_load(config_file)

# Logging configuration
log_config = conf['logging']
logging.config.dictConfig(log_config)
logger = logging.getLogger(__name__)


# input/output configuration
inputs = conf['inputs']
outputs = conf['outputs']


def main() -> None:
    app_name = conf['appname']
    logger.info(f'Starting spark application: {app_name}')
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()

    sch = StructType([
        StructField('id', IntegerType(), False),
        StructField('name', StringType(), True),
        StructField('location', StringType(), True)
    ])


    logger.info(f'Reading files and Creating DFs')
    inc_df = spark.read \
        .format('csv') \
        .option('delimiter', ',') \
        .schema(sch) \
        .load(inputs['tech_inc'])

    mp_df = spark.read \
        .format('csv') \
        .option('delimiter', ',') \
        .schema(sch) \
        .load(inputs['mptech'])

    logger.info('Joining DFs')
    joined_df = inc_df \
        .join(
            broadcast(mp_df),
            on=['id', 'name', 'location'],
            how='full'
        )

    logger.info(f'Writing files')
    inc_df.write \
        .mode('overwrite') \
        .parquet(outputs['tech_inc'])
    mp_df.write \
        .mode('overwrite') \
        .parquet(outputs['mptech'])
    joined_df.write \
        .mode('overwrite') \
        .parquet(outputs['merged'])

    logger.info('Exit application\n')
    spark.stop()


if __name__ == '__main__':
    main()
