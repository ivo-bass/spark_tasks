import sys
import os

sys.path.insert(0, os.path.abspath('..'))

from grad_task.classes import CsvWriter, ParquetWriter


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
