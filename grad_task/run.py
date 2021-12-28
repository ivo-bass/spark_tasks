"""
cd script dir and
run script with:

spark-submit run.py config.yml >> info.log

"""

from time import sleep
from datetime import datetime

import databricks.koalas as ks

from classes import ArgsParser, Configurator, SparkManager, Inputs, Outputs, Logger
from functions import read_and_build_dataframes, write_files, get_last_checkin_date, get_name_and_id_for_elite_users, \
    get_count_of_reviews_per_user, create_elite_reviews_df_sorted_by_count, get_average_stars_and_id_per_business, \
    get_reviews_count_per_business, join_business_stars_and_reviews_count, get_worst_10_businesses, \
    get_best_10_businesses, get_most_useful_reviews, parse_dates, get_first_year_of_elite, create_elite_since_df


def main() -> None:
    configurations = ArgsParser().parse_configurations()
    configurator = Configurator(**configurations)
    spark_manager = SparkManager(configurator.spark)
    inputs = Inputs(**configurator.inputs)
    outputs = Outputs(**configurator.outputs)
    logger = Logger(configurator.logging).logger
    spark = spark_manager.start_spark(logger=logger)

    # The following settings avoid errors and warnings due to legacy koalas version on the test server
    # to use different dfs in the same statement
    ks.set_option('compute.ops_on_diff_frames', True)
    # to solve 'WARN WindowExec: No Partition Defined for Window operation!'
    ks.options.compute.default_index_type = 'distributed-sequence'

    # ___ BUILD DFs ___
    business, user, review, full_review, tip, checkin = read_and_build_dataframes(
        logger=logger, spark=spark, inputs=inputs
    )

    # ___ TRANSFORM DFs ___
    # checkin
    last_checkin = checkin.drop('date')
    last_checkin['last_checkin'] = get_last_checkin_date(ser=checkin['date'], logger=logger)

    # review and user
    elite_users = get_name_and_id_for_elite_users(kdf=user, logger=logger)
    reviews_count = get_count_of_reviews_per_user(kdf=review, logger=logger)
    elite_reviews = create_elite_reviews_df_sorted_by_count(
        left=elite_users, right=reviews_count, logger=logger)

    # review
    business_stars = get_average_stars_and_id_per_business(kdf=review, logger=logger)
    count_per_business = get_reviews_count_per_business(kdf=review, logger=logger)
    stars_and_count = join_business_stars_and_reviews_count(
        left=business_stars, right=count_per_business, logger=logger)
    worst10 = get_worst_10_businesses(kdf=stars_and_count, logger=logger)
    best10 = get_best_10_businesses(kdf=stars_and_count, logger=logger)
    most_useful_reviews = get_most_useful_reviews(kdf=review, logger=logger)

    # tip
    tip['date'] = parse_dates(ser=tip['date'], logger=logger)

    # user
    first_year = get_first_year_of_elite(ser=user['elite'], logger=logger)
    elite_since = create_elite_since_df(kdf=user, ser=first_year, logger=logger)

    # full review
    logger.info('Rename user extended columns')
    user_extended = user.drop('yelping_since').set_index('user_id')
    user_extended = user_extended.rename(columns={'name': 'user_name', 'elite': 'user_elite'})
    logger.info('Rename business extended columns')
    business_extended = business.set_index('business_id').rename(columns={'name': 'business_name'})
    logger.info('Concatenating business address')
    business_extended['business_address'] = \
        business_extended.state + ', ' + \
        business_extended.city + ', ' + \
        business_extended.address + ', ' + \
        business_extended.postal_code
    logger.info('Dropping address parts')
    business_extended = business_extended.drop(['state', 'city', 'address', 'postal_code'])

    logger.info('Converting date to datetime')
    full_review['date'] = ks.to_datetime(full_review['date']).dt.floor('D', nonexistent='shift_backward')
    full_review = full_review.rename(columns={
        'stars': 'review_stars',
        'useful': 'review_useful',
        'text': 'review_text',
        'date': 'review_date'})
    logger.info('Joining reviews with users')
    full_review = full_review.join(user_extended, on="user_id", how="left")
    logger.info('Joining reviews with business')
    full_review = full_review.join(business_extended, on='business_id', how='left')
    # _____________________________________________

    # ___ WRITE DFs ___
    write_files(last_checkin, elite_reviews, worst10, best10, count_per_business,
                most_useful_reviews, tip, elite_since, full_review,
                logger=logger, outputs=outputs)
    # _______________________________________________

    logger.info("SUCCESS!\n")
    spark.stop()


if __name__ == '__main__':
    main()
