# Databricks notebook source
from pyspark.sql import DataFrame

# COMMAND ----------

"""
    Check if a file exists at the specified path

    Args:
        path (str): The path to the file

    Returns:
        bool: True if the file exists, False otherwise

    Raises:
        Exception: An error occurred during the execution, indicating a problem other than file not found
"""

def file_exists(path: str) -> bool:
  try:
    dbutils.fs.ls(path)
    return True
  except Exception as e:
    if 'java.io.FileNotFoundException' in str(e):
      return False
    else:
      raise

# COMMAND ----------

"""
Write a DataFrame to a table.

Args:
    df (DataFrame): The DataFrame to be written to the table
    write_mode (str): The mode for writing the data. Supported modes are 'append', 'overwrite', etc
    write_format (str): The format in which the data should be written. For example, 'parquet', 'delta', etc
    schema_name (str): The name of the schema where the table will be created
    table_name (str): The name of the table to which the DataFrame will be written

Raises:
    Exception: An error occurred during the execution

Note:
    This function writes the DataFrame to the specified table using the provided write mode and format. If an error occurs during the writing process, the exception is printed, and the error is raised

Example:
    >>> write_table(df, 'overwrite', 'delta', 'default', 'my_table')
"""

def write_table(df: DataFrame, write_mode: str, write_format: str, schema_name: str, table_name: str) -> None:
    try:
        df.write.mode(write_mode).format(write_format).saveAsTable(f"{schema_name}.{table_name}")
    except Exception as e:
        print(e)
        raise

# COMMAND ----------

"""
    Create a table containing a range of dates

    Returns:
        None: This function does not return any value

    Raises:
        Exception: An error occurred during the execution

    Note:
        This function creates a table with a range of dates from the earliest possible date (1970-01-01) to the current date

    Example:
        >>> create_dates_table()
"""

def create_dates_table() -> None:
    FIRST_POSSIBLE_DATE = '1970-01-01'
    LAST_POSSIBLE_DATE = datetime.date.today().strftime("%Y-%m-%d")

    try:
        df_dates = spark.createDataFrame([(FIRST_POSSIBLE_DATE, LAST_POSSIBLE_DATE)], ["start_date", "end_date"])
        df_dates = df_dates.selectExpr("explode(sequence(to_date(start_date), to_date(end_date))) as date")
        write_table(df_dates, "overwrite", "delta", "default", "dates")
        print ("Dates table created!!!")
    except Exception as e:
        print (e)
        raise