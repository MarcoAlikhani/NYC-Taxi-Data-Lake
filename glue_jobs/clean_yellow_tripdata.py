"""
nyc-yellow-clean-etl-script  (revised after reading the crawler output)

Reads raw_yellow_tripdata from the Glue Catalog. The raw table is a single
unpartitioned Parquet file with ~3M rows; this job adds the partitioning
(pickup_year, pickup_month), drops garbage rows, enriches with
trip_duration_minutes, and writes Snappy Parquet to the clean zone —
registering clean_yellow_tripdata in the Catalog on the fly.
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql import functions as F

# ---- 1. Job setup ----
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ---- 2. Configuration ----
SRC_DATABASE  = "nyc_taxi_raw"
SRC_TABLE     = "raw_yellow_tripdata"
DST_DATABASE  = "nyc_taxi_clean"
DST_TABLE     = "clean_yellow_tripdata"
DST_S3_PATH   = "s3://marco-data-darya/clean/yellow_tripdata/"   # <-- your bucket

# ---- 3. Read raw data ----
raw_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=SRC_DATABASE,
    table_name=SRC_TABLE,
    transformation_ctx="raw_dyf",
)
df = raw_dyf.toDF()

# ---- 4. Clean ----
# 4a. Drop rows missing critical fields
df = df.dropna(subset=[
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "fare_amount",
    "total_amount",
])

# 4b. Filter obvious garbage rows
df = df.filter(
    (F.col("fare_amount")     > 0) &
    (F.col("total_amount")    > 0) &
    (F.col("trip_distance")   > 0) &
    (F.col("passenger_count") > 0) &
    (F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime"))
)

# 4c. (REMOVED) The passenger_count cast — it's already bigint per the crawler.
# 4d. (REMOVED) The drop of inherited `year`/`month` columns — the raw table has no partition keys.

# ---- 5. Enrich ----
df = (
    df
    .withColumn("pickup_year",  F.year(F.col("tpep_pickup_datetime")))
    .withColumn("pickup_month", F.month(F.col("tpep_pickup_datetime")))
    .withColumn(
        "trip_duration_minutes",
        (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60.0,
    )
)

# 5b. SANITY FILTER on derived columns — the raw data has rows with timestamps
#     from 2008 and 2087 (data quality bugs upstream). We only keep "plausible"
#     years to avoid creating partition folders for 1970, 2087, etc.
df = df.filter(
    (F.col("pickup_year") >= 2009) & (F.col("pickup_year") <= 2025)
)

# ---- 6. Repartition for sane output file sizes ----
# The input is ONE file → ONE Spark partition → most of the cluster sits idle
# AND we'd write tiny files into many output partitions.
# Repartitioning by the output partition columns:
#   - parallelises the work across workers
#   - guarantees one output file per (pickup_year, pickup_month) combination
df = df.repartition("pickup_year", "pickup_month")

# ---- 7. Convert to DynamicFrame for the catalog-aware write ----
clean_dyf = DynamicFrame.fromDF(df, glueContext, "clean_dyf")

# ---- 8. Write Parquet, partitioned, register in Catalog ----
sink = glueContext.getSink(
    path=DST_S3_PATH,
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["pickup_year", "pickup_month"],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="sink",
)
sink.setCatalogInfo(catalogDatabase=DST_DATABASE, catalogTableName=DST_TABLE)
sink.setFormat("glueparquet")
sink.writeFrame(clean_dyf)

# ---- 9. Commit ----
job.commit()