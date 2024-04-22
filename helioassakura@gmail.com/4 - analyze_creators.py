# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql import DataFrame
from pyspark.sql.types import StructField, StringType, IntegerType, LongType, TimestampType, ArrayType
import datetime

# COMMAND ----------

# MAGIC %run /Users/helioassakura@gmail.com/utils

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Definitions

# COMMAND ----------

users_yt = spark.read.table("default.users_yt")
posts_creator = spark.read.table("default.posts_creator")

# COMMAND ----------

display(users_yt)

# COMMAND ----------

display(posts_creator)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Exercises

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 4.1 Top 3 most liked posted videos for each creator in last 6 months 

# COMMAND ----------

"""
    Get the top 3 liked posts by creator within the last 6 months

    Args:
        df_users (DataFrame): DataFrame containing user data
        df_posts (DataFrame): DataFrame containing posted videos data
        like_column (str): Column name representing number of likes
        creator_column (str): Column name representing creators name

    Returns:
        DataFrame: DataFrame with the top 3 liked posts by creator within the last 6 months, including user ID, video title, number of likes, and rank

    Example:
        get_top_3_liked_posts_by_creator(users_yt, posts_creator, "likes", "user_id")
"""

def get_top_3_liked_posts_by_creator(df_users: DataFrame, df_posts: DataFrame, like_column: str, creator_column: str) -> DataFrame:

        window_most_likes_order_column = like_column
        window_most_likes_partition_column = creator_column

        window_most_likes = Window.partitionBy(window_most_likes_partition_column).orderBy(desc(window_most_likes_order_column))

        return df_users \
                .join(df_posts, df_users["user_id"] == df_posts["yt_user"], "left") \
                .filter(df_posts["published_at_timestamp"] > add_months(current_date(), -6)) \
                .withColumn("rank", rank().over(window_most_likes)) \
                .filter(col("rank") <= 3) \
                .select("user_id", "title", "likes", "rank")

display(get_top_3_liked_posts_by_creator(users_yt, posts_creator, "likes", "user_id"))

# COMMAND ----------

display(get_top_3_liked_posts_by_creator(users_yt, posts_creator, "likes", "user_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 4.2 Top 3 most viewed posted videos for each creator in last 6 months 

# COMMAND ----------

"""
    Get the top 3 viewed posts by creator within the last 6 months

    Args:
        df_users (DataFrame): DataFrame containing user data
        df_posts (DataFrame): DataFrame containing posted videos data
        view_column (str): Column name representing total views
        creator_column (str): Column name representing creators name

    Returns:
        DataFrame: DataFrame with the top 3 viewed posts by creator within the last 6 months, including user ID, video title, number of views, and rank

    Example:
        >>> get_top_3_viewed_posts_by_creator(users_yt, posts_creator, "views", "user_id")
"""

def get_top_3_viewed_posts_by_creator(df_users: DataFrame, df_posts: DataFrame, view_column: str, creator_column: str) -> DataFrame:

        window_most_viewed_order_column = view_column
        window_most_viewed_partition_column = creator_column

        window_most_viewed = Window.partitionBy(window_most_viewed_partition_column).orderBy(desc(window_most_viewed_order_column))

        return df_users \
                .join(df_posts, df_users["user_id"] == df_posts["yt_user"], "left") \
                .filter(df_posts["published_at_timestamp"] > add_months(current_date(), -6)) \
                .withColumn("rank", rank().over(window_most_viewed)) \
                .filter(col("rank") <= 3) \
                .select("user_id", "title", "views", "rank")

display(get_top_3_viewed_posts_by_creator(users_yt, posts_creator, "views", "user_id"))

# COMMAND ----------

display(get_top_3_viewed_posts_by_creator(users_yt, posts_creator, "views", "user_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC > **Comment**
# MAGIC >
# MAGIC > Having a graph showing the results is very important. For example, we can see that the most liked video (1.7 MM) is not the most watched (68MM).
# MAGIC >
# MAGIC > Checking the title, we can see that the channel with the most watched video is a "children" content channel, and looks like that there's not much interaction. For example, the most liked video from `portadosfundos` **has the same number of likes** of checkgate's most liked video, but **less than one third of the views**.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 4.3 yt_users that are in default.post_creator, but not in default.users_yt table

# COMMAND ----------

"""
    Get creators who are not present in the users table

    Args:
        df_users (DataFrame): DataFrame containing users data
        df_posts (DataFrame): DataFrame containing posted videos data

    Returns:
        DataFrame: DataFrame with creators who are not present in the users table

    Example:
        >>> get_creators_not_in_users_table(users_yt, posts_creator)
"""

def get_creators_not_in_users_table(df_users: DataFrame, df_posts: DataFrame) -> DataFrame:
    
    df_distinct_users = df_posts.select("yt_user").distinct()

    return df_distinct_users \
            .join(df_users, df_distinct_users["yt_user"] == df_users["user_id"], "left_anti") \
            .select("yt_user")

display(get_creators_not_in_users_table(users_yt, posts_creator))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 4.4 Get the number of posts for each month and creator

# COMMAND ----------

"""
    Get the total number of posts by month and creator

    Args:
        df_users (DataFrame): DataFrame containing user data
        df_posts (DataFrame): DataFrame containing posted videos data

    Returns:
        DataFrame: DataFrame with the total number of posts by month and creator

    Example:
        >>> get_total_posts_by_month_and_creator(users_yt, posts_creator)
"""

def get_total_posts_by_month_and_creator(df_users: DataFrame, df_posts: DataFrame) -> DataFrame:

    # In case theres duplicated videos posted at the same time, we need to deduplicate the data
    return df_users \
            .join(df_posts, df_users["user_id"] == df_posts["yt_user"], "left") \
            .withColumn("month", date_trunc('month', col('published_at_timestamp'))) \
            .dropDuplicates(["user_id", "published_at", "title"]) \
            .groupBy("user_id", "month") \
            .agg(
                countDistinct(col("title")).alias("total_posts")
            ) \
            .select("user_id", "month", "total_posts") \
            .orderBy("user_id", "month")

display(get_total_posts_by_month_and_creator(users_yt, posts_creator))

# COMMAND ----------

display(get_total_posts_by_month_and_creator(users_yt, posts_creator))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 4.4.1 Extra 1: Show 0 when there's no video on that month

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC #### Creating date table

# COMMAND ----------

create_dates_table()

# COMMAND ----------

"""
    Get the month when the first video was posted by creator

    Args:
        df_posts_creator (DataFrame): DataFrame containing videos posted by creator

    Returns:
        DataFrame: DataFrame with the month when the first video was posted for all creators

    Raises:
        Exception: An error occurred during the execution, and the details are printed before raising the exception
"""

def get_first_posted_video_month(df_posts_creator: DataFrame) -> DataFrame:
    
    try:
        return df_posts_creator \
        .groupby() \
        .agg(
            min(col("published_at_timestamp")).alias("min_published_at_timestamp")
        ) \
        .withColumn("month_min_published_at_timestamp", date_trunc("month", col("min_published_at_timestamp"))) \
        .drop("min_published_at_timestamp")
    except Exception as e:
        print (e)
        raise

"""
    Get the total posts with all months for each user, starting from the first month where a video was posted

    Args:
        df_users (DataFrame): DataFrame containing user data
        df_months_filtered (DataFrame): DataFrame containing months from first posted video to current date
        df_posts_by_month (DataFrame): DataFrame containing posts count by month for each user

    Returns:
        DataFrame: DataFrame with the total posts for each user across all months, including months with no posts

    Raises:
        Exception: An error occurred during the execution, and the details are printed before raising the exception
"""

def get_total_posts_with_all_months(df_users: DataFrame, df_months_filtered: DataFrame, df_posts_by_month: DataFrame) -> DataFrame:
    
    try:
        return df_users.join(df_months_filtered) \
            .join(df_posts_by_month, ["user_id", "month"], "left") \
            .drop("wiki_page") \
            .withColumn("total_posts", coalesce(col("total_posts"), lit(0))) \
            .orderBy("user_id", "month")
    except Exception as e:
        print(e)
        raise

# COMMAND ----------

df_dates_table = spark.read.table("default.dates") \
    .withColumn("month", date_trunc("month", col("date"))) \
    .select("month").distinct()

df_first_published_timestamp = get_first_posted_video_month(posts_creator)

df_months_filtered = \
    df_first_published_timestamp.join(df_dates_table, df_first_published_timestamp["month_min_published_at_timestamp"] <= df_dates_table["month"], "left") \
    .select("month")

df_posts_by_month = get_total_posts_by_month_and_creator(users_yt, posts_creator)

display(get_total_posts_with_all_months(users_yt, df_months_filtered, df_posts_by_month))

# COMMAND ----------

display(get_total_posts_with_all_months(users_yt, df_months_filtered, df_posts_by_month))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 4.4.2 Extra 2: Transform the table into a format where the first column is the user_id, and we have one column for each month.

# COMMAND ----------

"""
    Pivot the DataFrame to generate a pivoted table

    Args:
        df (DataFrame): The DataFrame to be pivoted
        group_by_column (str): The column to group by
        pivot_column (str): The column to pivot
        agg_column (str): The column to aggregate

    Returns:
        DataFrame: The pivoted DataFrame containing aggregated values (sum)

    Raises:
        Exception: An error occurred during the execution, and the details are printed before raising the exception
"""

def with_pivoted_table(df: DataFrame, group_by_column: str, pivot_column: str, agg_column: str) -> DataFrame:
    try:
        return df.groupBy(group_by_column).pivot(pivot_column).sum(agg_column)
    except Exception as e:
        print (e)
        raise

df_total_posts = get_total_posts_with_all_months(users_yt, df_months_filtered, df_posts_by_month) \
    .withColumn("month", date_format(col("month"),"yyyy/MM"))

display(with_pivoted_table(df_total_posts, "user_id", "month", "total_posts"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 4.4.3 Extra 3: Show the 3 most used tags for each content creator

# COMMAND ----------

"""
    Get the top 3 used tags by creator. If there's more than one tag with the same number of occurrences, it will be grouped in an Array

    Args:
        df_users (DataFrame): DataFrame containing user data
        df_posts (DataFrame): DataFrame containing posted videos data
        total_tags_column (str): Column name representing the total tags count
        creator_column (str): Column name representing creators column to partition

    Returns:
        DataFrame: DataFrame with the top 3 used tags by creator, along with their occurrences

    Raises:
        Exception: An error occurred during the execution

    Note:
        This function handles cases where there are many tags with the same number of occurrences by putting them in an array to provide a better view

    Example:
        >>> get_top_3_used_tags_by_creator(df_users, df_posts, "total_tags", "creator_id")
"""

def get_top_3_used_tags_by_creator(df_users: DataFrame, df_posts: DataFrame, total_tags_column: str, creator_column: str) -> DataFrame:

    window_tags_order_column = total_tags_column
    window_tags_partition_column = creator_column

    window_tags = Window.partitionBy(window_tags_partition_column).orderBy(desc(window_tags_order_column))

    # If there's too many tags with the same number of occurrences, they will be put in an array to be able to get a better view.

    return df_users \
            .join(df_posts, df_users["user_id"] == df_posts["yt_user"], "left") \
            .withColumn("tag", explode_outer(col("tags"))) \
            .groupBy("user_id", "tag") \
            .agg(
                count(col("tag")).alias(total_tags_column)
            ) \
            .groupBy("user_id", total_tags_column) \
            .agg(
                collect_list('tag').alias('tag')
            ) \
            .withColumn("rank", rank().over(window_tags)) \
            .filter(col("rank") <= 3)

display(get_top_3_used_tags_by_creator(users_yt, posts_creator, "total_tag_usage", "user_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Tests

# COMMAND ----------

import pyspark.testing
from pyspark.testing.utils import assertDataFrameEqual

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 4.1

# COMMAND ----------

# get_top_3_liked_posts_by_creator should return the top 3 most videos posts for each creator for last 6 months
def test_get_top_3_liked_posts_by_creator():
    df_users = spark.createDataFrame(
        data = [
            ("user_1", "wiki_page_1"),
            ("user_2", "wiki_page_2")
            ], schema=["user_id", "wiki_page"]
        )

    df_posts = spark.createDataFrame(
        data = [
            ("user_1", datetime.datetime(2024, 3, 16, 12, 34, 56), "title_1", 20),
            ("user_1", datetime.datetime(2024, 2, 16, 12, 34, 56), "title_2", 25),
            ("user_2", datetime.datetime(2023, 3, 16, 12, 34, 56), "title_3", 50),
            ("user_3", datetime.datetime(2024, 3, 16, 12, 34, 56), "title_4", 70)
            ], schema = ["yt_user", "published_at_timestamp", "title", "likes"]
        )

    expected_schema = StructType([
            StructField('user_id', StringType()),
            StructField('title', StringType()),
            StructField('likes', LongType()),
            StructField('rank', IntegerType())
    ])

    df_expected = spark.createDataFrame(
        data = [
            ("user_1", "title_2", 25, 1),
            ("user_1", "title_1", 20, 2)
        ], schema = expected_schema
    )

    df_result = get_top_3_liked_posts_by_creator(df_users, df_posts, "likes", "user_id")

    print(f"Running test_get_top_3_liked_posts_by_creator...")
    return assertDataFrameEqual(df_result, df_expected)

test_get_top_3_liked_posts_by_creator()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 4.2

# COMMAND ----------

# get_top_3_viewed_posts_by_creator should return the top 3 most viewed videos for each creator for last 6 months

def test_get_top_3_viewed_posts_by_creator():
    df_users = spark.createDataFrame(
        data = [
            ("user_1", "wiki_page_1"),
            ("user_2", "wiki_page_2")
            ], schema=["user_id", "wiki_page"]
        )

    df_posts = spark.createDataFrame(
        data = [
            ("user_1", datetime.datetime(2024, 2, 16, 12, 34, 56), "title_1", 20),
            ("user_1", datetime.datetime(2024, 2, 16, 12, 34, 56), "title_2", 25),
            ("user_1", datetime.datetime(2024, 2, 16, 12, 34, 56), "title_3", 30),
            ("user_1", datetime.datetime(2024, 3, 16, 12, 34, 56), "title_4", 35),
            ("user_2", datetime.datetime(2023, 3, 16, 12, 34, 56), "title_5", 50),
            ("user_3", datetime.datetime(2024, 3, 16, 12, 34, 56), "title_6", 70)
            ], schema = ["yt_user", "published_at_timestamp", "title", "views"]
        )

    expected_schema = StructType([
            StructField('user_id', StringType()),
            StructField('title', StringType()),
            StructField('views', LongType()),
            StructField('rank', IntegerType())
    ])

    df_expected = spark.createDataFrame(
        data = [
            ("user_1", "title_4", 35, 1),
            ("user_1", "title_3", 30, 2),
            ("user_1", "title_2", 25, 3)
        ], schema = expected_schema
    )

    df_result = get_top_3_viewed_posts_by_creator(df_users, df_posts, "views", "user_id")

    print(f"Running test_get_top_3_viewed_posts_by_creator...")
    return assertDataFrameEqual(df_result, df_expected)

test_get_top_3_viewed_posts_by_creator()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 4.3

# COMMAND ----------

# get_creators_not_in_users_table should return the creators that are in the posts table not in the users one

def test_get_creators_not_in_users_table():
    df_users = spark.createDataFrame(
        data = [
            ("user_1", "wiki_page_1"),
            ("user_2", "wiki_page_2"),
            ("user_3", "wiki_page_3"),
            ("user_4", "wiki_page_4")
            ], schema=["user_id", "wiki_page"]
        )

    df_posts = spark.createDataFrame(
        data = [
            ("user_1", datetime.datetime(2024, 3, 16, 12, 34, 56), "title_1"),
            ("user_1", datetime.datetime(2024, 2, 16, 12, 34, 56), "title_2"),
            ("user_1", datetime.datetime(2024, 2, 16, 12, 34, 56), "title_3"),
            ("user_1", datetime.datetime(2024, 2, 16, 12, 34, 56), "title_4"),
            ("user_2", datetime.datetime(2023, 3, 16, 12, 34, 56), "title_5"),
            ("user_3", datetime.datetime(2024, 3, 16, 12, 34, 56), "title_6"),
            ("user_5", datetime.datetime(2023, 3, 16, 12, 34, 56), "title_7")
            ], schema = ["yt_user", "published_at_timestamp", "title"]
        )

    expected_schema = StructType([
            StructField('yt_user', StringType())
    ])

    df_expected = spark.createDataFrame(
        data = [
            ("user_5",)
        ], schema = expected_schema
    )

    df_result = get_creators_not_in_users_table(df_users, df_posts)

    print(f"Running test_get_creators_not_in_users_table...")
    return assertDataFrameEqual(df_result, df_expected)

test_get_creators_not_in_users_table()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 4.4

# COMMAND ----------

# get_total_posts_by_month_and_creator should get the total monthly posted videos for each creator, ordered by user and month

def test_get_total_posts_by_month_and_creator():
    df_users = spark.createDataFrame(
        data = [
            ("user_1", "wiki_page_1"),
            ("user_2", "wiki_page_2")
            ], schema=["user_id", "wiki_page"]
        )

    df_posts = spark.createDataFrame(
        data = [
            ("user_1", 123456, datetime.datetime(2024, 2, 16, 12, 34, 56), "title_1"),
            ("user_1", 123457, datetime.datetime(2024, 2, 16, 12, 34, 56), "title_2"),
            ("user_1", 123458, datetime.datetime(2024, 2, 16, 12, 34, 56), "title_3"),
            ("user_1", 123458, datetime.datetime(2024, 2, 16, 12, 34, 56), "title_3"),
            ("user_1", 981232, datetime.datetime(2024, 3, 16, 12, 34, 56), "title_4"),
            ("user_2", 123123, datetime.datetime(2023, 3, 16, 12, 34, 56), "title_5"),
            ("user_3", 129387, datetime.datetime(2024, 3, 16, 12, 34, 56), "title_6")
            ], schema = ["yt_user", "published_at", "published_at_timestamp", "title"]
        )
    
    

    expected_schema = StructType([
            StructField('user_id', StringType()),
            StructField('month', TimestampType()),
            StructField('total_posts', LongType())
    ])

    df_expected = spark.createDataFrame(
        data = [
            ("user_1", datetime.datetime(2024, 2, 1, 0, 0, 0), 3),
            ("user_1", datetime.datetime(2024, 3, 1, 0, 0, 0), 1),
            ("user_2", datetime.datetime(2023, 3, 1, 0, 0, 0), 1)
        ], schema = expected_schema
    )

    df_result = get_total_posts_by_month_and_creator(df_users, df_posts)

    print(f"Running test_get_total_posts_by_month_and_creator...")
    return assertDataFrameEqual(df_result, df_expected)
    
test_get_total_posts_by_month_and_creator()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 4.4.1

# COMMAND ----------

# get_first_posted_video should get the timestamp of the first posted video in the posts table

def test_get_first_posted_video():

    df_posts_creator = spark.createDataFrame(
        data = [
            ("user_1", datetime.datetime(2024, 2, 16, 12, 34, 56)),
            ("user_1", datetime.datetime(2024, 2, 16, 12, 34, 56)),
            ("user_1", datetime.datetime(2024, 2, 16, 12, 34, 56)),
            ("user_1", datetime.datetime(2024, 3, 16, 12, 34, 56)),
            ("user_2", datetime.datetime(2023, 3, 16, 12, 34, 56)),
            ("user_3", datetime.datetime(2024, 3, 16, 12, 34, 56))
            ], schema = ["yt_user", "published_at_timestamp"]
        )
    
    expected_schema = StructType([
            StructField('month_min_published_at_timestamp', TimestampType())
    ])

    df_expected = spark.createDataFrame(
        data = [
            (datetime.datetime(2023, 3, 1, 0, 0, 0), )
        ], schema = expected_schema
    )

    df_result = get_first_posted_video_month(df_posts_creator)

    print(f"Running test_get_first_posted_video...")
    return assertDataFrameEqual(df_result, df_expected)

# get_total_posts_with_all_months should show the posts table with all the possible months, starting from the first post's month

def test_get_total_posts_with_all_months():

    df_users = spark.createDataFrame(
        data = [
            ("user_1", "wiki_page_1"),
            ("user_2", "wiki_page_2"),
            ("user_3", "wiki_page_3")
            ], schema = ["user_id", "wiki_page"]
        )
    
    months_schema = StructType([
            StructField('month', TimestampType())
    ])
    
    df_months_filtered = spark.createDataFrame(
        data = [
            (datetime.datetime(2024, 2, 1, 00, 00, 00),),
            (datetime.datetime(2024, 3, 1, 00, 00, 00),),
            (datetime.datetime(2024, 4, 1, 00, 00, 00),)
        ], schema = months_schema
    )    

    df_posts_by_month = spark.createDataFrame(
        data = [
            ("user_1", datetime.datetime(2024, 2, 1, 00, 00, 00), 20),
            ("user_1", datetime.datetime(2024, 3, 1, 00, 00, 00), 30),
            ("user_1", datetime.datetime(2024, 4, 1, 00, 00, 00), 40),
            ("user_2", datetime.datetime(2024, 2, 1, 00, 00, 00), 50),
            ("user_3", datetime.datetime(2024, 3, 1, 00, 00, 00), 60)
            ], schema = ["user_id", "month", "total_posts"]
        )
    
    expected_schema = StructType([
            StructField('user_id', StringType()),
            StructField('month', TimestampType()),
            StructField('total_posts', LongType())
    ])

    df_expected = spark.createDataFrame(
        data = [
            ("user_1", datetime.datetime(2024, 2, 1, 0, 0, 0), 20),
            ("user_1", datetime.datetime(2024, 3, 1, 0, 0, 0), 30),
            ("user_1", datetime.datetime(2024, 4, 1, 0, 0, 0), 40),
            ("user_2", datetime.datetime(2024, 2, 1, 0, 0, 0), 50),
            ("user_2", datetime.datetime(2024, 3, 1, 0, 0, 0), 0),
            ("user_2", datetime.datetime(2024, 4, 1, 0, 0, 0), 0),
            ("user_3", datetime.datetime(2024, 2, 1, 0, 0, 0), 0),
            ("user_3", datetime.datetime(2024, 3, 1, 0, 0, 0), 60),
            ("user_3", datetime.datetime(2024, 4, 1, 0, 0, 0), 0)
        ], schema = expected_schema
    )

    df_result = get_total_posts_with_all_months(df_users, df_months_filtered, df_posts_by_month)

    print(f"Running test_get_total_posts_with_all_months...")
    return assertDataFrameEqual(df_result, df_expected)

test_get_first_posted_video()
test_get_total_posts_with_all_months()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 4.4.2

# COMMAND ----------

# with_pivoted_table should show a pivoted total posts by creator table, with the dates formatted as yyyy/MM

def test_with_pivoted_table():
    
    df_total_posts = spark.createDataFrame(
        data = [
            ("user_1", '2024/02', 20),
            ("user_1", '2024/03', 30),
            ("user_1", '2024/04', 40),
            ("user_2", '2024/02', 50),
            ("user_2", '2024/03', 0),
            ("user_2", '2024/04', 0),
            ("user_3", '2024/02', 0),
            ("user_3", '2024/03', 60),
            ("user_3", '2024/04', 0)
        ], schema = ["user_id", "month", "total_posts"]
        )
    
    expected_schema = StructType([
            StructField('user_id', StringType()),
            StructField('2024/02', LongType()),
            StructField('2024/03', LongType()),
            StructField('2024/04', LongType())
    ])

    df_expected = spark.createDataFrame(
        data = [
            ("user_1", 20, 30, 40),
            ("user_2", 50, 0, 0),
            ("user_3", 0, 60, 0)
        ], schema = expected_schema
    )

    df_result = with_pivoted_table(df_total_posts, "user_id", "month", "total_posts")

    print(f"Running test_with_pivoted_table...")
    return assertDataFrameEqual(df_result, df_expected)
test_with_pivoted_table()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 4.4.3

# COMMAND ----------

# get_top_3_used_tags_by_creator should return the top 3 most used tags for each creator

def test_get_top_3_used_tags_by_creator():

    df_users = spark.createDataFrame(
        data = [
            ("user_1", "wiki_page_1"),
            ("user_2", "wiki_page_2"),
            ("user_3", "wiki_page_3")
            ], schema=["user_id", "wiki_page"]
        )

    df_posts = spark.createDataFrame(
        data = [
            ("user_1", ["tag1", "tag2", "tag3"]),
            ("user_1", ["tag1", "tag2", "tag4"]),
            ("user_1", ["tag1", "tag2", "tag4"]),
            ("user_2", ["tag1", "tag5", "tag6"]),
            ("user_2", ["tag1", "tag5"]),
            ("user_2", ["tag1"]),
            ("user_3", ["tag7", "tag8", "tag9"]),
            ("user_3", ["tag9"]),
            ("user_3", ["tag8", "tag9"])
            ], schema = ["yt_user", "tags"]
        )

    expected_schema = StructType([
            StructField('user_id', StringType()),
            StructField('total_tag_usage', LongType()),
            StructField("tag",ArrayType(StringType())),
            StructField('rank', IntegerType())
    ])

    df_expected = spark.createDataFrame(
        data = [
            ("user_1", 3, ["tag1", "tag2"], 1),
            ("user_1", 2, ["tag4"], 2),
            ("user_1", 1, ["tag3"], 3),
            ("user_2", 3, ["tag1"], 1),
            ("user_2", 2, ["tag5"], 2),
            ("user_2", 1, ["tag6"], 3),
            ("user_3", 3, ["tag9"], 1),
            ("user_3", 2, ["tag8"], 2),
            ("user_3", 1, ["tag7"], 3)
        ], schema = expected_schema
    )

    df_result = get_top_3_used_tags_by_creator(df_users, df_posts, "total_tag_usage", "user_id")

    print(f"Running test_get_top_3_used_tags_by_creator...")
    return assertDataFrameEqual(df_result, df_expected)

test_get_top_3_used_tags_by_creator()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Running all tests

# COMMAND ----------

def run_all_tests():
    test_get_top_3_liked_posts_by_creator()
    test_get_top_3_viewed_posts_by_creator()
    test_get_creators_not_in_users_table()
    test_get_total_posts_by_month_and_creator()
    # 2 tests for the same problem: test_get_first_posted_video and test_get_total_posts_with_all_months
    test_get_first_posted_video()
    test_get_total_posts_with_all_months()
    test_with_pivoted_table()
    test_get_top_3_used_tags_by_creator()

run_all_tests()