def read_and_build_dataframes(inputs, builder=None, json_to_spark=None, json_to_koalas=None):
    """Reads files and builds koalas.DataFrames specific for this application.
    :param inputs: Inputs instance
    :param builder: KdfBuilder instance
    :param json_to_spark: json to spark reader
    :param json_to_koalas: json to koalas reader
    :return: tuple[koalas.DataFrames]
    """
    business = builder.build(
        path=inputs.business,
        reader=json_to_spark,
        columns=['business_id', 'name', 'address',
                 'city', 'state', 'postal_code']
    )
    user = builder.build(
        path=inputs.user,
        reader=json_to_spark,
        columns=['user_id', 'name', 'yelping_since', 'elite']
    )
    review_sdf = json_to_spark.read(path=inputs.review)
    review = builder.build(
        sdf=review_sdf,
        columns=['review_id', 'user_id', 'business_id', 'stars', 'useful']
    )
    full_review = builder.build(
        sdf=review_sdf,
        columns=['business_id', 'user_id', 'stars', 'useful', 'text', 'date']
    )
    return business, user, review, full_review
