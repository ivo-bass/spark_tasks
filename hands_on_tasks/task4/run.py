"""
Run script with:

spark-submit run.py config.yml >> info.log

"""
import argparse
import logging.config
from datetime import datetime

import pandas as pd
import databricks.koalas as ks
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


def main() -> None:
    app_name = conf['appname']
    logger.info(f'Starting spark application: {app_name}')
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()

    # ______________________

    ks.set_option('compute.default_index_type', 'distributed')

    input_file = inputs['patients']
    logger.info(f'Reading from {input_file} and Creating Koalas DF')
    kdf = ks.read_csv(
        path=input_file,
        index_col='patientID'
    )

    logger.info('Converting string to datetime')
    kdf['dateOfBirth'] = kdf.dateOfBirth.apply(ks.to_datetime)
    kdf['lastVisitDate'] = kdf.lastVisitDate.apply(ks.to_datetime)

    start_date = datetime.strptime('2012-09-15', '%Y-%m-%d')
    end_date = datetime.today().date()

    logger.info("Find all the patients whose lastVisitDate between current time and '2012-09-15'")
    filt = (kdf.lastVisitDate > start_date) & (kdf.lastVisitDate < end_date)
    current_visits_df = kdf[filt]
    print(current_visits_df.head())

    logger.info("Find all the patients who born in 2011")
    filt2 = kdf['dateOfBirth'].dt.year == 2011
    born_2011_df = kdf[filt2]
    print(born_2011_df.head())

    today = pd.Timestamp.now()

    def calculate_age(born: pd.Timestamp) -> int:
        return today.year - born.year - ((today.month, today.day) < (born.month, born.day))

    logger.info("Find all the patients age")
    kdf['age'] = kdf['dateOfBirth'].apply(calculate_age)
    print(kdf.head())

    logger.info("Select patients 18 years old or younger")
    filt3 = kdf['age'] <= 18
    non_adult_df = kdf[filt3]
    print(non_adult_df.head())

    # ______________________

    logger.info('Exit application\n')
    spark.stop()


if __name__ == '__main__':
    main()
