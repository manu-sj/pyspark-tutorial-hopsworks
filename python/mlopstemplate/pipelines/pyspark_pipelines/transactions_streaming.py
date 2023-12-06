from pyspark.sql import SparkSession

from pyspark.sql.functions import (
    from_json,
    window,
    avg,
    count,
    stddev,
    explode,
    date_format,
    col,
    mean,
    pandas_udf,
    PandasUDFType)

from pyspark.sql.types import (
    LongType,
    DoubleType,
    StringType,
    TimestampType,
    StructType,
    StructField,
)

import hopsworks
import hsfs
from hsfs import engine

from mlopstemplate.features import transactions

# Name of the Kafka topic to read card transactions from. Introduced in previous notebook.
KAFKA_TOPIC_NAME = "credit_card_transactions"

# define spark context
spark = SparkSession.builder.enableHiveSupport().getOrCreate()
spark_context = spark.sparkContext
#ssc = StreamingContext(spark_context, 2)

# connect to hopsworks
project = hopsworks.login()
fs = project.get_feature_store()
hsfs.connection()
kafka_config = engine.get_instance()._get_kafka_config(feature_store_id=fs.id)

# get data from the source
df_read = spark \
    .readStream \
    .format("kafka") \
    .options(**kafka_config) \
    .option("startingOffsets", "earliest") \
    .option("subscribe", KAFKA_TOPIC_NAME) \
    .load()

parse_schema = StructType([StructField("tid", StringType(), True),
                           StructField("datetime", TimestampType(), True),
                           StructField("cc_num", LongType(), True),
                           StructField("category", StringType(), True),
                           StructField("amount", DoubleType(), True),
                           StructField("latitude", DoubleType(), True),
                           StructField("longitude", DoubleType(), True),
                           StructField("city", StringType(), True),
                           StructField("country", StringType(), True),
                           ])

# Deserialize data from and create streaming query
transaction_streaming_df = df_read.selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", parse_schema).alias("value")) \
    .select("value.tid",
            "value.datetime",
            "value.cc_num",
            "value.category",
            "value.amount",
            "value.latitude",
            "value.longitude",
            "value.city",
            "value.country") \
    .selectExpr("CAST(tid as string)",
                "CAST(datetime as timestamp)",
                "CAST(cc_num as long)",
                "CAST(category as string)",
                "CAST(amount as double)",
                "CAST(latitude as double)",
                "CAST(longitude as double)",
                "CAST(city as string)",
                "CAST(country as string)"
                )

schema1 = StructType([StructField('tid', StringType(), True),
                      StructField('datetime', TimestampType(), True),
                      StructField('cc_num', LongType(), True),
                      StructField('category', StringType(), True),
                      StructField('amount', DoubleType(), True),
                      StructField('latitude', DoubleType(), True),
                      StructField('longitude', DoubleType(), True),
                      StructField('city', StringType(), True),
                      StructField('country', StringType(), True),
                      StructField('loc_delta_t_minus_1', DoubleType(), True)
                      ])

schema2 = StructType([StructField('tid', StringType(), True),
                      StructField('datetime', TimestampType(), True),
                      StructField('cc_num', LongType(), True),
                      StructField('category', StringType(), True),
                      StructField('amount', DoubleType(), True),
                      StructField('latitude', DoubleType(), True),
                      StructField('longitude', DoubleType(), True),
                      StructField('city', StringType(), True),
                      StructField('country', StringType(), True),
                      StructField('loc_delta_t_minus_1', DoubleType(), True),
                      StructField('time_delta_t_minus_1', DoubleType(), True),
                      ])

schema3 = StructType([StructField('tid', StringType(), True),
                      StructField('datetime', TimestampType(), True),
                      StructField('cc_num', LongType(), True),
                      StructField('category', StringType(), True),
                      StructField('amount', DoubleType(), True),
                      StructField('latitude', DoubleType(), True),
                      StructField('longitude', DoubleType(), True),
                      StructField('city', StringType(), True),
                      StructField('country', StringType(), True),
                      StructField('loc_delta_t_minus_1', DoubleType(), True),
                      StructField('time_delta_t_minus_1', DoubleType(), True),
                      StructField('month', StringType(), True),
                      StructField('age_at_transaction', DoubleType(), True),
                      ])

schema4 = StructType([StructField('tid', StringType(), True),
                      StructField('datetime', TimestampType(), True),
                      StructField('cc_num', LongType(), True),
                      StructField('category', StringType(), True),
                      StructField('amount', DoubleType(), True),
                      StructField('latitude', DoubleType(), True),
                      StructField('longitude', DoubleType(), True),
                      StructField('city', StringType(), True),
                      StructField('country', StringType(), True),
                      StructField('loc_delta_t_minus_1', DoubleType(), True),
                      StructField('time_delta_t_minus_1', DoubleType(), True),
                      StructField('month', StringType(), True),
                      StructField('age_at_transaction', DoubleType(), True),
                      StructField('days_until_card_expires', DoubleType(), True),
                      ])


udf = pandas_udf(f=transactions.get_year_month, returnType="string")

# read profile data to cogroup with streaming dataframe
profile_fg = fs.get_or_create_feature_group(
    name="profile",
    version=1)

profile_df = profile_fg.read()
# streamingDf.join(staticDf, "cc_num")  # inner equi-join with static DF
# streamingDf.join(staticDf, "cc_num", "right_join")  # right outer join with a static DF
# streamingDf.join(staticDf, "cc_num", "left_join")  # left outer join with a static DF

# 7 days window  #.groupBy(window("datetime", "168 hours"), "cc_num")
windowed_transaction_df = transaction_streaming_df \
    .selectExpr("tid",
                "datetime",
                "cc_num",
                "category",
                "CAST(amount as double)",
                "radians(latitude) as latitude",
                "radians(longitude) as longitude",
                "city",
                "country") \
    .withWatermark("datetime", "24 hours") \
    .groupBy(window("datetime", "168 hours")) \
    .applyInPandas(transactions.loc_delta_t_minus_1, schema=schema1) \
    .withWatermark("datetime", "24 hours") \
    .groupBy(window("datetime", "168 hours")) \
    .applyInPandas(transactions.time_delta_t_minus_1, schema=schema2) \
    .withColumn("month", udf(col("datetime"))) \
    .withWatermark("datetime", "24 hours") \
    .groupBy(window("datetime", "168 hours")) \
    .cogroup(profile_df.groupby("cc_provider")) \
    .applyInPandas(transactions.card_owner_age, schema=schema3) \
    .withWatermark("datetime", "24 hours") \
    .groupBy(window("datetime", "168 hours")) \
    .cogroup(profile_df.groupby("cc_provider")) \
    .applyInPandas(transactions.expiry_days, schema=schema4)

# get or create feature group
trans_fg = fs.get_or_create_feature_group(
    name="transactions",
    version=1,
    description="Transaction data",
    primary_key=['cc_num'],
    event_time='datetime',
    partition_key=['month'],
    stream=True,
    online_enabled=True
)

# materialize feature data in to the online feature group
q = trans_fg.insert_stream(windowed_transaction_df)

# make sure job runs continuously
q.awaitTermination()
