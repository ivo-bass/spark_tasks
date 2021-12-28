from datetime import datetime

import databricks.koalas as ks


def clean_dates_and_get_max(date_str):
    dates = date_str.split(', ')
    return ks.to_datetime(dates, infer_datetime_format=True).max()


def split_string_and_get_last_date(ser, logger):
    logger.info('Cleaning dates and returning last date')
    return ser \
        .map(clean_dates_and_get_max) \
        .dt.floor('D', nonexistent='shift_backward') \
        .dt.date


def get_name_and_id_for_elite_users(kdf, logger):
    logger.info('Filter elite users and return new kdf with username and id')
    elite_filter = kdf['elite'].str.len() > 0
    return kdf.loc[elite_filter, ['name', 'user_id']]


def group_by__count__rename(kdf, grp_col: str, count_col: str, new_name: str, logger):
    logger.info(f'Grouping by {grp_col}, counting {count_col} and renaming to {new_name}')
    return kdf.groupby(grp_col)[count_col].count().rename(new_name)


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
    useful = kdf.groupby('user_id')['useful'].sum().rename(
        'useful_reviews').reset_index()
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


def drop_and_rename_columns(kdf, drop_cols: list, new_idx: str, columns: dict, logger):
    logger.info(f'Drop {drop_cols}, set {new_idx} as index and rename columns')
    new_kdf = kdf.drop(drop_cols).set_index(new_idx)
    return new_kdf.rename(columns=columns)


def set_index_and_rename_columns(kdf, new_idx: str, columns: dict, logger):
    logger.info(f'Set index to {new_idx} and rename columns')
    return kdf.set_index(new_idx).rename(columns=columns)


def concatenate_columns(kdf, logger):
    logger.info('Concatenating business address')
    kdf['business_address'] = \
        kdf.state + ', ' + \
        kdf.city + ', ' + \
        kdf.address + ', ' + \
        kdf.postal_code
    logger.info('Dropping address parts')
    return kdf.drop(
        ['state', 'city', 'address', 'postal_code'])


def join_left_with_2_dfs(left, right1, on1, right2, on2, logger):
    logger.info('Joining 3 dfs')
    left = left.join(right1, on=on1, how="left")
    left = left.join(right2, on=on2, how='left')
    return left
