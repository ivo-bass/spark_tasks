def write_files(elite_reviews, worst10, best10,
                count_per_business, most_useful_reviews, elite_since, full_review,
                outputs, csv_writer, parquet_writer) -> None:
    """Writes all specific to this application DataFrames to files [parquet, csv]
    :param elite_reviews
    :param worst10: koalas.DataFrame
    :param best10: koalas.DataFrame
    :param count_per_business: koalas.DataFrame
    :param most_useful_reviews: koalas.DataFrame
    :param elite_since: koalas.DataFrame
    :param full_review: koalas.DataFrame
    :param outputs: Outputs instance
    :param csv_writer: CsvWriter instance
    :param parquet_writer: ParquetWriter instance
    :return: None
    """
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
    csv_writer.write(
        kdf=elite_since,
        path=outputs.elite_user
    )
    parquet_writer.write(
        kdf=full_review,
        path=outputs.full_review,
        partition_cols=full_review.review_date.dt.year
    )
