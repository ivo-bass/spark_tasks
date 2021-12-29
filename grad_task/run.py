"""
run script with:

spark-submit --driver-memory 6g run.py config.yml >> info.log

"""

from classes import ArgsParser, Configurator, SparkManager, Inputs, Outputs, Logger
from functions import read_and_build_dataframes, write_files
from functions.functions import *


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

    # read, cache, transform, write and uncache the dfs which has no contribution to the other dfs
    # tip
    tip_sdf = spark.read.json(path=inputs.tip)
    sdf_columns = tip_sdf.select('user_id', 'business_id', 'text', 'date')
    tip_uncached = ks.DataFrame(data=sdf_columns)
    with tip_uncached.spark.cache() as tip:
        tip['date'] = parse_dates(ser=tip['date'], logger=logger)
        tip.to_parquet(path=outputs.tip, partition_cols=tip['date'].dt.year)

    # checkin
    checkin_uncached = ks.read_json(path=inputs.checkin)
    with checkin_uncached.spark.cache() as checkin:
        checkin['last_checkin'] = split_string_and_get_last_date(
            ser=checkin['date'], logger=logger)
        checkin = checkin.drop('date')
        checkin.to_parquet(path=outputs.checkin, partition_cols=checkin['last_checkin'].dt.year)

    # ___ BUILD DFs ___
    business, user, review, full_review = read_and_build_dataframes(
        logger=logger, spark=spark, inputs=inputs
    )
    business = business.spark.cache()
    user = user.spark.cache()
    review = review.spark.cache()

    # ___ TRANSFORM DFs ___

    # review and user
    elite_users = filter_empty_strings_and_get_columns(
        kdf=user, filt_col='elite', columns=['name', 'user_id'], logger=logger)
    reviews_count = group_by__count__rename(
        kdf=review, grp_col='user_id', count_col='review_id', new_name='number_of_reviews', logger=logger)
    elite_reviews = join__sort_values(
        left=elite_users, right=reviews_count, on='user_id', how='left',
        sort_by="number_of_reviews", ascending=False, logger=logger)

    # review
    business_stars = group_by__mean__rename__reset_index(
        kdf=review, grp_col='business_id', mean_col='stars', new_name='average_stars', logger=logger)
    count_per_business = group_by__count__rename(
        kdf=review, grp_col='business_id', count_col='review_id', new_name='reviews_count', logger=logger)
    stars_and_count = join_inner(
        left=business_stars, right=count_per_business, on="business_id", logger=logger)
    worst10 = sort__head__drop(
        kdf=stars_and_count, sort_col='average_stars', ascending=True, n_head=10, drop_col='reviews_count',
        logger=logger)
    # We need to do this because pd.DataFrame.tail() is not implemented in the current koalas version ('1.0.1')
    best10 = sort__head__drop(
        kdf=stars_and_count, sort_col='average_stars', ascending=False, n_head=10, drop_col='reviews_count',
        logger=logger)
    most_useful_reviews = group_by__sum__rename__reset_index__sort__head(
        kdf=review, grp_col='user_id', sum_col='useful', new_name='useful_reviews',
        sort_col='useful_reviews', ascending=False, n_head=10, logger=logger)

    # user
    first_year = split__get_first__to_datetime__year(
        ser=user['elite'], logger=logger)
    elite_since = create_elite_since_df(
        kdf=user, ser=first_year, logger=logger)

    # full review
    user_extended = drop_and_rename_columns(
        kdf=user, drop_cols=['yelping_since'], new_idx='user_id',
        columns={'name': 'user_name', 'elite': 'user_elite'}, logger=logger)

    business_extended = set_index_and_rename_columns(
        kdf=business, new_idx='business_id', columns={'name': 'business_name'}, logger=logger)
    business_extended = concatenate_columns(
        kdf=business_extended, logger=logger)

    full_review['date'] = parse_dates(ser=full_review['date'], logger=logger)
    full_review = full_review.rename(columns={
        'stars': 'review_stars',
        'useful': 'review_useful',
        'text': 'review_text',
        'date': 'review_date'})
    full_review = join_left_with_2_dfs(
        left=full_review, right1=user_extended, on1='user_id',
        right2=business_extended, on2='business_id', logger=logger)

    # ___ WRITE DFs ___
    write_files(elite_reviews, worst10, best10, count_per_business,
                most_useful_reviews, elite_since, full_review,
                logger=logger, outputs=outputs)

    logger.info("SUCCESS!\n")
    spark.stop()


if __name__ == '__main__':
    main()
