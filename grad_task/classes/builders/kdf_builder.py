import databricks.koalas as ks
from pyspark.sql import DataFrame


class KdfBuilder:
    @staticmethod
    def build(columns: list, sdf: DataFrame = None, reader=None) -> ks.DataFrame:
        if reader and not sdf:
            sdf = reader.read()
        sdf_columns = sdf.select(columns)
        return ks.DataFrame(data=sdf_columns)
