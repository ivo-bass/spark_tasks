"""
Run script with:

spark-submit run.py config.yml >> info.log

"""

import argparse
import logging.config
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
outputs = conf['outputs']


def main() -> None:
    app_name = conf['appname']
    logger.info(f'Starting spark application: {app_name}')
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()

    ks.set_option('compute.ops_on_diff_frames', True)

    logger.info('# 1. Load all the files into Koalas DataFrames')
    products_kdf = ks.read_csv(inputs['products'])
    sales_kdf = ks.read_csv(inputs['sales'])
    sellers_kdf = ks.read_csv(inputs['sellers'])

    logger.info('# 2. Find how many products, sales, and sellers we have in the dataset')
    products_count = products_kdf['product_id'].count()
    sales_count = sales_kdf['order_id'].count()
    sellers_count = sellers_kdf['seller_id'].count()

    logger.info('# 3. How many unique products are sold each day?')
    sales_kdf['date'] = sales_kdf['date'].apply(ks.to_datetime)
    unique_products_count_by_date = sales_kdf.groupby(['date'])['product_id'].nunique().to_frame()

    logger.info('# 4. What is the average revenue of all orders?')
    sales_with_price = sales_kdf.merge(products_kdf, on="product_id", how='left')
    sales_with_price['revenue'] = sales_with_price['num_pieces_sold'] * sales_with_price['price']
    average_revenue = sales_with_price.revenue.mean()

    logger.info('# 5. Who is the most selling and the least selling person (Seller)?')
    sellers_sales = sales_kdf.groupby('seller_id')['num_pieces_sold'].sum().to_frame()
    best_seller_id = sellers_sales.num_pieces_sold.idxmax()
    worst_seller_id = sellers_sales.num_pieces_sold.idxmin()

    logger.info('# 6. Which is the order with the highest profit?')
    best_order_index = sales_with_price.revenue.idxmax()

    logger.info('# 7. Create a new column in sales DF.'
                ' If the profit from a sale is higher than 50 '
                'then set the profit to high else set it to low.')

    def eval_profit(n: int) -> str:
        return 'high' if n > 50 else 'low'

    sales_kdf['profit_level'] = sales_with_price['revenue'].map(eval_profit)

    logger.info('# 8. Save all the results in parquet format')
    ll = [
        products_count,
        sales_count,
        sellers_count,
        average_revenue,
        best_seller_id,
        worst_seller_id,
        best_order_index
    ]
    idx = [
        'products_count',
        'sales_count',
        'sellers_count',
        'average_revenue',
        'best_seller_id',
        'worst_seller_id',
        'best_order_index'
    ]
    statistics_kdf = ks.DataFrame(ll, index=idx)
    statistics_kdf.to_parquet(path=outputs['statistics'])

    unique_products_count_by_date.to_parquet(path=outputs['unique_products'])

    sales_kdf.to_parquet(path=outputs['sales'])

    logger.info('Exit application\n')
    spark.stop()


if __name__ == '__main__':
    main()
