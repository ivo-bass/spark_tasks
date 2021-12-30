"""
run script with:

spark-submit --driver-memory 6g run.py config.yml >> info.log

"""
import databricks.koalas as ks

from core import ArgsParser, Configurator, SparkManager, Inputs, Outputs, Logger, \
    JsonToKoalasReader, JsonToSparkReader, KdfBuilder, Transform, read_and_build_dataframes,\
    write_files, CsvWriter, ParquetWriter


def main() -> None:
    configurations = ArgsParser().parse_configurations()
    configurator = Configurator(**configurations)
    spark_manager = SparkManager(configurator.spark)
    inputs = Inputs(**configurator.inputs)
    outputs = Outputs(**configurator.outputs)
    logger = Logger(configurator.logging).logger
    spark = spark_manager.start_spark(logger=logger)
    json_to_koalas = JsonToKoalasReader(logger=logger)
    json_to_spark = JsonToSparkReader(logger=logger, spark=spark)
    transform = Transform(logger=logger)
    builder = KdfBuilder()
    csv_writer = CsvWriter(logger=logger)
    parquet_writer = ParquetWriter(logger=logger)

    # The following settings avoid errors and warnings due to legacy koalas version on the test server
    # to use different dfs in the same statement
    ks.set_option('compute.ops_on_diff_frames', True)
    # to solve 'WARN WindowExec: No Partition Defined for Window operation!'
    ks.options.compute.default_index_type = 'distributed-sequence'

    # read, cache, transform, write and un-cache the dfs which has no contribution to the other dfs
    # tip
    tip_uncached = builder.build(
        path=inputs.tip,
        reader=json_to_spark,
        columns=['user_id', 'business_id', 'text', 'date']
    )
    with tip_uncached.spark.cache() as tip:
        tip['date'] = transform.parse_dates(ser=tip['date'])
        tip.to_parquet(path=outputs.tip, partition_cols=tip['date'].dt.year)

    # checkin
    checkin_uncached = json_to_koalas.read(path=inputs.checkin)
    with checkin_uncached.spark.cache() as checkin:
        checkin['last_checkin'] = transform.split_string__get_last_date(ser=checkin['date'])
        checkin = checkin.drop('date')
        checkin.to_parquet(path=outputs.checkin, partition_cols=checkin['last_checkin'].dt.year)

    # ___ BUILD DFs ___
    business, user, review, full_review = read_and_build_dataframes(
        inputs=inputs, builder=builder, json_to_spark=json_to_spark
    )
    business = business.spark.cache()
    user = user.spark.cache()
    review = review.spark.cache()

    # ___ TRANSFORM DFs ___

    # review and user
    elite_users = transform.filter_empty_strings_and_get_columns(
        kdf=user, filter_col='elite', columns=['name', 'user_id'])
    reviews_count = transform.group_by__count__rename(
        kdf=review, grp_col='user_id', count_col='review_id', new_name='number_of_reviews')
    elite_reviews = transform.join__sort_values(
        left=elite_users, right=reviews_count, on='user_id', how='left',
        sort_by="number_of_reviews", ascending=False)

    # review
    business_stars = transform.group_by__mean__rename__reset_index(
        kdf=review, grp_col='business_id', mean_col='stars', new_name='average_stars')
    count_per_business = transform.group_by__count__rename(
        kdf=review, grp_col='business_id', count_col='review_id', new_name='reviews_count')
    stars_and_count = transform.join_inner(
        left=business_stars, right=count_per_business, on="business_id")
    worst10 = transform.sort__head__drop(
        kdf=stars_and_count, sort_col='average_stars', ascending=True, n_head=10, drop_col='reviews_count')
    # We need to do this because pd.DataFrame.tail() is not implemented in the current koalas version ('1.0.1')
    best10 = transform.sort__head__drop(
        kdf=stars_and_count, sort_col='average_stars', ascending=False, n_head=10, drop_col='reviews_count')
    most_useful_reviews = transform.group_by__sum__rename__reset_index__sort__head(
        kdf=review, grp_col='user_id', sum_col='useful', new_name='useful_reviews',
        sort_col='useful_reviews', ascending=False, n_head=10)

    # user
    first_year = transform.split__get_first__to_datetime__year(ser=user['elite'])
    elite_since = transform.create_elite_since_df(kdf=user, ser=first_year)

    # full review
    user_extended = transform.drop__rename_columns(
        kdf=user, drop_cols=['yelping_since'], new_idx='user_id',
        columns={'name': 'user_name', 'elite': 'user_elite'})

    business_extended = transform.set_index__rename_columns(
        kdf=business, new_idx='business_id', columns={'name': 'business_name'})
    business_extended = transform.concatenate_columns(kdf=business_extended)

    full_review['date'] = transform.parse_dates(ser=full_review['date'])
    full_review = full_review.rename(columns={
        'stars': 'review_stars',
        'useful': 'review_useful',
        'text': 'review_text',
        'date': 'review_date'})
    full_review = transform.join_left_with_2_dfs(
        left=full_review, right1=user_extended, on1='user_id',
        right2=business_extended, on2='business_id')

    # ___ WRITE DFs ___
    write_files(elite_reviews, worst10, best10, count_per_business,
                most_useful_reviews, elite_since, full_review,
                outputs=outputs, csv_writer=csv_writer, parquet_writer=parquet_writer)

    logger.info("SUCCESS!\n")
    spark.stop()


if __name__ == '__main__':
    main()
