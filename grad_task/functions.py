from datetime import datetime

import databricks.koalas as ks
from classes import KdfBuilder, JsonToSparkReader, JsonToKoalasReader, CsvWriter, ParquetWriter


def read_and_build_dataframes(logger, spark, inputs):
    builder = KdfBuilder()
    json_to_spark_reader = JsonToSparkReader(logger=logger, spark=spark)
    business = builder.build(
        path=inputs.business,
        reader=json_to_spark_reader,
        columns=['business_id', 'name', 'address', 'city', 'state', 'postal_code']
    )
    user = builder.build(
        path=inputs.user,
        reader=json_to_spark_reader,
        columns=['user_id', 'name', 'yelping_since', 'elite']
    )
    review_sdf = JsonToSparkReader(logger=logger, spark=spark).read(path=inputs.review)
    review = builder.build(
        sdf=review_sdf,
        columns=['review_id', 'user_id', 'business_id', 'stars', 'useful']
    )
    full_review = builder.build(
        sdf=review_sdf,
        columns=['business_id', 'user_id', 'stars', 'useful', 'text', 'date']
    )
    tip = builder.build(
        path=inputs.tip,
        reader=json_to_spark_reader,
        columns=['user_id', 'business_id', 'text', 'date']
    )
    checkin = JsonToKoalasReader(logger=logger).read(path=inputs.checkin)
    return business, user, review, full_review, tip, checkin


def write_files(last_checkin, elite_reviews, worst10, best10,
                count_per_business, most_useful_reviews, tip, elite_since, full_review,
                logger, outputs):
    csv_writer = CsvWriter(logger=logger)
    parquet_writer = ParquetWriter(logger=logger)
    parquet_writer.write(
        kdf=last_checkin,
        path=outputs.checkin,
        partition_cols=last_checkin['last_checkin'].dt.year
    )
    csv_writer.write(
        kdf=elite_reviews,
        path=outputs.review_text_elite_reviews
    )
    csv_writer.write(
        kdf=worst10,
        path=outputs.review_text_worst_businesses
    )
    csv_writer.write(
        kdf=best10,
        path=outputs.review_text_best_businesses
    )
    csv_writer.write(
        kdf=count_per_business,
        path=outputs.review_text_count_reviews
    )
    csv_writer.write(
        kdf=most_useful_reviews,
        path=outputs.review_text_most_useful
    )
    parquet_writer.write(
        kdf=tip,
        path=outputs.tip,
        partition_cols=tip['date'].dt.year
    )
    csv_writer.write(
        kdf=elite_since,
        path=outputs.elite_user
    )
    parquet_writer.write(
        kdf=full_review,
        path=outputs.full_review,
        partition_cols=full_review['review_date'].dt.year
    )


def clean_dates_and_get_max(date_str):
    dates = date_str.split(', ')
    return ks.to_datetime(dates, infer_datetime_format=True).max()


def get_last_checkin_date(ser, logger):
    logger.info('Cleaning dates and returning last date')
    return ser \
        .map(clean_dates_and_get_max) \
        .dt.floor('D', nonexistent='shift_backward') \
        .dt.date


def get_name_and_id_for_elite_users(kdf, logger):
    logger.info('Filter elite users and return new kdf with username and id')
    elite_filter = kdf['elite'].str.len() > 0
    return kdf.loc[elite_filter, ['name', 'user_id']]


def get_count_of_reviews_per_user(kdf, logger):
    logger.info('Getting the reviews count per user')
    return kdf.groupby('user_id')['review_id'].count().rename("number_of_reviews")


def create_elite_reviews_df_sorted_by_count(left, right, logger):
    logger.info('Joining elite users with reviews count and sorting by count')
    return left.join(right, on="user_id", how="left") \
        .sort_values(by="number_of_reviews", ascending=False)


def get_average_stars_and_id_per_business(kdf, logger):
    logger.info('Calculate average business stars')
    groups = kdf.groupby('business_id')
    return groups['stars'].mean().rename('average_stars').reset_index()


def get_reviews_count_per_business(kdf, logger):
    logger.info('Counting reviews count per business')
    groups = kdf.groupby('business_id')
    return groups['review_id'].count().rename('reviews_count')


def join_business_stars_and_reviews_count(left, right, logger):
    logger.info('Joining business stars with reviews count')
    return left.join(right, on="business_id")


def get_worst_10_businesses(kdf, logger):
    logger.info('Sorting stars and count ascending and returning first 10 rows')
    worst10 = kdf.sort_values('average_stars').head(10)
    return worst10.drop('reviews_count')


def get_best_10_businesses(kdf, logger):
    # We need to sort again this time descending in order to get first 10 for the best businesses
    # We need to do this because pd.DataFrame.tail() is not implemented in the current koalas version ('1.0.1')
    logger.info('Sorting stars and count descending and returning first 10 rows')
    best10 = kdf.sort_values('average_stars', ascending=False).head(10)
    return best10.drop('reviews_count')


def get_most_useful_reviews(kdf, logger):
    logger.info('Group reviews by user, sum useful reviews and get top 10')
    useful = kdf.groupby('user_id')['useful'].sum().rename('useful_reviews').reset_index()
    return useful.sort_values('useful_reviews', ascending=False).head(10)


def parse_dates(ser, logger):
    logger.info('Convert string to datetime')
    return ks.to_datetime(ser, infer_datetime_format=True)\
        .dt.floor('D', nonexistent='shift_backward')


def get_first_year_of_elite(ser, logger):
    logger.info('Split datetimes of elite users and get the first year')
    return ks.to_datetime(ser.str.split(',').str.get(0), errors='coerce').dt.year


def create_elite_since_df(kdf, ser, logger):
    today = datetime.now().year
    since = ks.DataFrame(kdf['user_id'])
    since['years_since_elite'] = ser.map(lambda x: today - x)
    return since
