# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import from_unixtime, col, when, count

# COMMAND ----------

# MAGIC %run ./utils

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Definitions

# COMMAND ----------


table_schema = StructType([
    StructField("yt_user",StringType(),False),
    StructField("creator_id",StringType(),True),
    StructField("published_at",IntegerType(),True),
    StructField("views",IntegerType(),True),
    StructField("likes",IntegerType(),True),
    StructField("title",StringType(),True),
    StructField("tags",ArrayType(StringType()),True),
    StructField("_corrupt_record", StringType(), True)
])

# COMMAND ----------

FILE_LOCATION = "/FileStore/tables/posts_creator_json.gz"

WRITE_MODE = "overwrite"
WRITE_FORMAT = "delta"
SCHEMA_NAME = "default"
TABLE_NAME = "posts_creator"

# COMMAND ----------

if file_exists(FILE_LOCATION):
    try:
        df = spark.read.schema(table_schema).option("columnNameOfCorruptRecord", "_corrupt_record").json(FILE_LOCATION)
        df_with_timestamp = df.withColumn("published_at_timestamp", from_unixtime(df["published_at"]).cast("timestamp"))
    except Exception as e:
        print (e)
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC > **Comment**
# MAGIC >
# MAGIC > The `option("columnNameOfCorruptRecord", "_corrupt_record")` option gets the data that somehow is not "valid". For example, a column is IntegerType and in the json file is a StringType. It will be `Null` in the table, but the "original" data will be in the `_corrupt_record` column as an Array. If the data is valid, `_corrupt_record` will be `Null`.
# MAGIC >
# MAGIC > Valid Data (string, int): ("string1", "int1") -> ("string1", "int1", null)
# MAGIC >
# MAGIC > Bad Data (string, int): ("string1", "string2") -> ("string1", null, {column1: "string1", column2: "string2"})

# COMMAND ----------

display(df_with_timestamp)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Checking data integrity

# COMMAND ----------

# Check for number of null values in Data

display(
    df_with_timestamp.select(
        [count(when(col(c).isNull(), c)).alias(c) for c in df_with_timestamp.columns if c != "_corrupt_record"]
    )
)

# COMMAND ----------

# Check duplicates (case sensitive)

assert (df_with_timestamp.select("yt_user", "creator_id", "published_at", "title", "tags").count() == df_with_timestamp.select("yt_user", "creator_id", "published_at", "title", "tags").distinct().count())

# COMMAND ----------

# Check if likes and views are non negative values

assert (df_with_timestamp.filter((col("likes") < 0) | (col("views") < 0)).count() == 0)

# COMMAND ----------

# Check if there's corrupted data

display(df_with_timestamp.filter(col("_corrupt_record").isNotNull()))

# COMMAND ----------

# Get general summary

display(df_with_timestamp.summary())

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC > **Comment**
# MAGIC >
# MAGIC > summary() only considers Strings and Numeric types. Timestamps are now shown

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Creating the table

# COMMAND ----------

df_with_timestamp = df_with_timestamp.drop("_corrupt_record")
write_table(df_with_timestamp, WRITE_MODE, WRITE_FORMAT, SCHEMA_NAME, TABLE_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC > **Comment**
# MAGIC >
# MAGIC > Since we are not using this notebook as a daily process, I didn't put a column like "ingested_at" to check the date of insertion
# MAGIC >
# MAGIC > **Another Comment**
# MAGIC >
# MAGIC > Another way to create this table is to create a table "raw", with the exact data that comes from the JSON files. I decided to create the "next" table, with a few more columns and possibly filtering some bad data

# COMMAND ----------

display(spark.read.table(f"{SCHEMA_NAME}.{TABLE_NAME}"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Tests

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Checks if table exists

# COMMAND ----------

assert spark.catalog.tableExists(f"{SCHEMA_NAME}.{TABLE_NAME}") == True

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Checks if table is not empty

# COMMAND ----------

assert spark.read.table(f"{SCHEMA_NAME}.{TABLE_NAME}").count() > 0