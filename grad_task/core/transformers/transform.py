from datetime import datetime

import databricks.koalas as ks


class Transform:
    """This class holds all transformation methods.

    The methods are named to be self-documenting:
        def method_1__method_2__...method_N():
    """

    def __init__(self, logger) -> None:
        self.logger = logger

    def drop__rename_columns(self, kdf, drop_cols: list, new_idx: str, columns: dict):
        self.logger.info(f'Drop {drop_cols}, set {new_idx} as index and rename columns')
        new_kdf = kdf.drop(drop_cols).set_index(new_idx)
        return new_kdf.rename(columns=columns)

    def filter_empty_strings_and_get_columns(self, kdf, filter_col: str, columns: list):
        self.logger.info('Filter elite users and return new kdf with username and id')
        filter_condition = kdf[filter_col].str.len() > 0
        return kdf.loc[filter_condition, columns]

    def group_by__sum__rename__reset_index__sort__head(self, kdf, grp_col: str, sum_col: str, new_name: str,
                                                       sort_col: str, ascending: bool, n_head: int):
        self.logger.info('Group reviews by user, sum useful reviews and get top 10')
        useful = kdf.groupby(grp_col)[sum_col].sum().rename(
            new_name).reset_index()
        return useful.sort_values(sort_col, ascending=ascending).head(n_head)

    def group_by__count__rename(self, kdf, grp_col: str, count_col: str, new_name: str):
        self.logger.info(f'Grouping by {grp_col}, counting {count_col} and renaming to {new_name}')
        return kdf.groupby(grp_col)[count_col].count().rename(new_name)

    def group_by__mean__rename__reset_index(self, kdf, grp_col: str, mean_col: str, new_name: str):
        self.logger.info(
            f'Grouping by {grp_col}, calculating mean on {mean_col}, renaming to {new_name} and resetting index')
        return kdf.groupby(grp_col)[mean_col].mean().rename(new_name).reset_index()

    def join__sort_values(self, left, right, on: str, how: str, sort_by: str, ascending: bool):
        self.logger.info(f'Joining on {on} and sorting by {sort_by}')
        return left.join(right, on=on, how=how) \
            .sort_values(by=sort_by, ascending=ascending)

    def join_inner(self, left, right, on: str):
        self.logger.info(f'Joining on {on}')
        return left.join(right, on=on)

    def join_left_with_2_dfs(self, left, right1, on1, right2, on2):
        self.logger.info('Joining 3 dfs')
        left = left.join(right1, on=on1, how="left")
        left = left.join(right2, on=on2, how='left')
        return left

    def parse_dates(self, ser):
        self.logger.info('Convert string to datetime')
        return ks.to_datetime(ser, infer_datetime_format=True) \
            .dt.floor('D', nonexistent='shift_backward')

    def set_index__rename_columns(self, kdf, new_idx: str, columns: dict):
        self.logger.info(f'Set index to {new_idx} and rename columns')
        return kdf.set_index(new_idx).rename(columns=columns)

    def sort__head__drop(self, kdf, sort_col: str, ascending: bool, n_head: int, drop_col: str):
        self.logger.info(f'Sorting by {sort_col} getting first {n_head} rows and dropping {drop_col}')
        worst10 = kdf.sort_values(sort_col, ascending=ascending).head(n_head)
        return worst10.drop(drop_col)

    def split_string__get_last_date(self, ser):
        self.logger.info('Cleaning dates and returning last date')
        return ser \
            .map(self.clean_dates__get_latest_datetime) \
            .dt.floor('D', nonexistent='shift_backward') \
            .dt.date

    def split__get_first__to_datetime__year(self, ser):
        self.logger.info('Split dates string and get the first year')
        return ks.to_datetime(ser.str.split(',').str.get(0), errors='coerce').dt.year

    def concatenate_columns(self, kdf):
        self.logger.info('Concatenating business address')
        kdf['business_address'] = \
            kdf.state + ', ' + \
            kdf.city + ', ' + \
            kdf.address + ', ' + \
            kdf.postal_code
        self.logger.info('Dropping address parts')
        return kdf.drop(
            ['state', 'city', 'address', 'postal_code'])

    @staticmethod
    def clean_dates__get_latest_datetime(date_str):
        dates = date_str.split(', ')
        return ks.to_datetime(dates, infer_datetime_format=True).max()

    def create_elite_since_df(self, kdf, ser):
        self.logger.info('Create new kdf and add new column with delta years')
        today = datetime.now().year
        new_kdf = ks.DataFrame(kdf['user_id'])
        new_kdf['years_since_elite'] = ser.map(lambda x: today - x)
        return new_kdf
