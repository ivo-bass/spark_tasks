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
