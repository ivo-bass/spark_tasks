"""
Run script with:

spark-submit run.py config.yml >> info.log

"""
import argparse
import logging.config

import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Setup arg parser for config file
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

    # Defining type schemas
    tech_sch = StructType([
        StructField(name='first', dataType=StringType()),
        StructField(name='last', dataType=StringType()),
        StructField(name='technology', dataType=StringType())
    ])

    sal_sch = StructType([
        StructField(name='first', dataType=StringType()),
        StructField(name='last', dataType=StringType()),
        StructField(name='salary', dataType=IntegerType())
    ])

    tech_input = inputs['tech']
    sal_input = inputs['salary']

    logger.info(f'Reading from {tech_input} and {sal_input} and Creating DFs')
    tech_df = spark.read \
        .format('csv') \
        .option('delimiter', ',') \
        .option('header', True) \
        .schema(tech_sch) \
        .load(tech_input)

    sal_df = spark.read \
        .format('csv') \
        .option('delimiter', ',') \
        .option('header', True) \
        .schema(sal_sch) \
        .load(sal_input)

    logger.info(f'Joining DFs and filtering salaries above 100_000')
    joined = sal_df.filter(sal_df['salary'] > 100_000).join(tech_df, ['first', 'last'])

    logger.info(f'Concatenating first and last name')
    concatenated = joined.select(
        concat_ws(
            ',',
            concat_ws(' ', joined['first'], joined['last']),
            joined['technology'],
            joined['salary']
        )
    )

    output_path = outputs['joined']
    logger.info(f'Writing file to {output_path}')
    concatenated.write \
        .mode('overwrite') \
        .format('text') \
        .save(output_path)

    logger.info('Exit application\n')
    spark.stop()


if __name__ == '__main__':
    main()
