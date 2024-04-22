# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import col, when, count

# COMMAND ----------

# MAGIC %run /Users/helioassakura@gmail.com/utils

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Definitions

# COMMAND ----------

table_schema = StructType([
    StructField("wiki_page",StringType(),False),
    StructField("_corrupt_record", StringType(), True)
])

# COMMAND ----------

FILE_LOCATION = "/FileStore/tables/wiki_pages_json.gz"

WRITE_MODE = "overwrite"
WRITE_FORMAT = "delta"
SCHEMA_NAME = "default"
TABLE_NAME = "creators_scrape_wiki"

# COMMAND ----------

if file_exists(FILE_LOCATION):
    try:
        df = spark.read.schema(table_schema).option("columnNameOfCorruptRecord", "_corrupt_record").json(FILE_LOCATION)
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

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Checking data integrity

# COMMAND ----------

# Check for number of null values in Data

display(
    df.select(
        [count(when(col(c).isNull(), c)).alias(c) for c in df.columns if c != "_corrupt_record"]
    )
)

# COMMAND ----------

# Check duplicates (case sensitive)

assert (df.select(col("wiki_page")).count() == df.select(col("wiki_page")).distinct().count())

# COMMAND ----------

# Check if there's corrupted data

display(df.filter(col("_corrupt_record").isNotNull()))

# COMMAND ----------

# Get general summary

display(df.summary())

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Creating the table

# COMMAND ----------

df = df.drop("_corrupt_record")
write_table(df, WRITE_MODE, WRITE_FORMAT, SCHEMA_NAME, TABLE_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC > **Comment**
# MAGIC >
# MAGIC > Since we are not using this notebook as a daily process, I didn't put a column like "ingested_at" to check the date of insertion

# COMMAND ----------

display(spark.read.table(f"{SCHEMA_NAME}.{TABLE_NAME}"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Checks

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

assert spark.read.table("default.creators_scrape_wiki").count() > 0