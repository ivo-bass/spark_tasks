import sys
import os

sys.path.insert(0, os.path.abspath('..'))

from grad_task.classes import KdfBuilder, JsonToSparkReader, JsonToKoalasReader


def read_and_build_dataframes(logger, spark, inputs):
    builder = KdfBuilder()
    json_to_spark_reader = JsonToSparkReader(logger=logger, spark=spark)
    business = builder.build(
        path=inputs.business,
        reader=json_to_spark_reader,
        columns=['business_id', 'name', 'address',
                 'city', 'state', 'postal_code']
    )
    user = builder.build(
        path=inputs.user,
        reader=json_to_spark_reader,
        columns=['user_id', 'name', 'yelping_since', 'elite']
    )
    review_sdf = JsonToSparkReader(
        logger=logger, spark=spark).read(path=inputs.review)
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
