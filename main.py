from pyspark.sql import SparkSession, DataFrame, Window
import pyspark.sql.functions as F
from datetime import datetime


def read_hard_drive_data(spark: SparkSession, data_location: str = '/data/*/*.csv') -> DataFrame:
    df = spark.read.csv(data_location, header='true')
    return df


def format_data(df: DataFrame) -> DataFrame:
    out_df = df.withColumn("date", F.to_date('date', 'yyyy-MM-dd'))
    return out_df


def calculate_failure_rates(df: DataFrame) -> DataFrame:
    metrics = df.groupBy('date').agg(F.sum(F.col('failure')).alias('failure_rate'))
    return metrics


def main():
    spark = SparkSession.builder.appName('HardDriveFailures') \
        .getOrCreate()
    t1 = datetime.now()
    df = read_hard_drive_data(spark=spark)
    df = format_data(df)
    metrics = calculate_failure_rates(df)
    metrics.write.parquet('metrics', mode='overwrite')
    t2 = datetime.now()
    metrics.show()
    print("it took {x}".format(x=t2-t1))


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()