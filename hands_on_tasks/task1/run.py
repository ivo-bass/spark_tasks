"""

Run script with:

spark-submit run.py config.yml >> info.log

"""
import argparse
import logging.config

import yaml
from pyspark.sql import SparkSession


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

    input_file = inputs['employee']
    logger.info(f'Reading input from {input_file}')
    df = spark.read.json(input_file)

    logger.info('Creating temp view')
    df.createOrReplaceTempView('people')
    query = 'SELECT last_name FROM people'

    logger.info('Executing sql query')
    last_name_df = spark.sql(query)

    output_file = outputs['last_name']
    logger.info(f'Writing file to {output_file}')
    last_name_df.write \
        .mode('overwrite') \
        .json(output_file)

    logger.info('Exit application\n')
    spark.stop()


if __name__ == '__main__':
    main()
