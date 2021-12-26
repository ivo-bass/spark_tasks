"""
cd script dir and
run script with:

spark-submit run.py config.yml >> info.log

"""

from time import sleep
from datetime import datetime

import databricks.koalas as ks

from classes import ArgsParser, Configurator, SparkManager, Inputs, Outputs, Logger
from functions import read_and_build_dataframes, write_files


configurations = ArgsParser().parse_configurations()
configurator = Configurator(**configurations)
spark_manager = SparkManager(configurator.spark)
inputs = Inputs(**configurator.inputs)
outputs = Outputs(**configurator.outputs)
logger = Logger(configurator.logging).logger


def clean_dates(date_str):
    dates = date_str.split(', ')
    return ks.to_datetime(dates, infer_datetime_format=True).max()


def main() -> None:
    spark = spark_manager.start_spark(logger=logger)

    ks.set_option('compute.ops_on_diff_frames', True)
    ks.options.compute.default_index_type = 'distributed-sequence'

    # ___ BUILD DFs ___
    business, user, review, full_review, tip, checkin = read_and_build_dataframes(
        logger=logger, spark=spark, inputs=inputs
    )
    # ___ TRANSFORM DFs ___
    logger.info('Apply clean dates to checkin df')

    last_checkins = checkin['date'] \
        .map(clean_dates) \
        .dt.floor('D', nonexistent='shift_backward') \
        .dt.date
    last_checkin = checkin.drop('date')
    last_checkin['last_checkin'] = last_checkins

    # ___________________________________________

    logger.info('Filter elite users')
    elite_filter = user['elite'].str.len() > 0
    elite_users = user.loc[elite_filter, ['name', 'user_id']]

    logger.info('Counting the reviews')
    user_groups = review.groupby('user_id')
    reviews_count = user_groups['review_id'] \
        .count() \
        .rename("number_of_reviews")

    logger.info('Join elite users with reviews count and sort by count')
    elite_reviews = elite_users \
        .join(reviews_count, on="user_id", how="left") \
        .sort_values(by="number_of_reviews", ascending=False)

    # ______________________________________________
    business_groups = review.groupby('business_id')
    logger.info('Calculate average business stars')
    business_stars = business_groups['stars'].mean().rename('average_stars').reset_index()
    # In order to compare businesses with the same average rating we may use the respective reviews counts
    logger.info('Counting reviews count per business')
    business_reviews_count = business_groups['review_id'].count().rename('reviews_count')
    logger.info("Sorting...Sorting...Sorting...")
    stars_and_count = business_stars.join(business_reviews_count, on="business_id")
    # First we'll sort the reviews count descending
    stars_and_count = stars_and_count.sort_values('reviews_count', ascending=False)

    # For the worst 1 we get first 10 of the ascending sorted stars
    worst10 = stars_and_count.sort_values('average_stars').head(10)
    # We need to sort again this time descending in order to get first 10 for the best businesses
    # We need to do this because pd.DataFrame.tail() is not implemented in the current koalas version ('1.0.1')
    best10 = stars_and_count.sort_values('average_stars', ascending=False).head(10)
    worst10 = worst10.drop('reviews_count')
    best10 = best10.drop('reviews_count')

    count_per_business = stars_and_count.drop('average_stars')

    logger.info('Get useful reviews')
    useful_reviews = user_groups['useful'].sum().rename('useful_reviews').reset_index()
    most_useful_reviews = useful_reviews.sort_values('useful_reviews', ascending=False).head(10)

    # ______________________________________________

    tip['date'] = ks.to_datetime(tip['date'], infer_datetime_format=True).dt.floor('D', nonexistent='shift_backward')

    # _____________________________________________
    logger.info('Calculate elite users years')
    today = datetime.now().year
    first_year = ks.to_datetime(user['elite'].str.split(',').str.get(0), errors='coerce').dt.year
    elite_since = ks.DataFrame(user['user_id'])
    elite_since['years_since_elite'] = first_year.map(lambda x: today - x)

    # _____________________________________________

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

    logger.info("SUCCESS!")
    sleep(10)
    logger.info('Exit application')
    spark.stop()


if __name__ == '__main__':
    main()
