# Case - Winnin

## The case

The instructions and the problems to solve can be found [here](https://github.com/winnin/desafio_dataeng/blob/main/README.md)

## Files

There are 5 notebooks:

1. **1 - create_table_creators_scrape_wiki**: Contains the code solving the Exercise number 1 of the case.
2. **2 - create_table_posts_creator**: Contains the code solving the Exercise number 2 of the case.
3. **3 - create_table_user_yt_from_wikipedia_api**: Contains the code solving the Exercise number 3 of the case.
4. **4 - analyze_creators**: Contains the code solving the Exercise number 4 (and extras) of the case. Beyond that, it has some graphs and Visualizations from Databricks Notebook. 
5. **utils**: Contains a few helper methods for all notebooks.

## Code

### Environment

The configs from the cluster that ran the notebooks are:

    Databricks Runtime Version
    > 14.3 LTS (includes Apache Spark 3.5.0, Scala 2.12)

    Driver type
    > Community Optimized
    > 15.3 GB Memory, 2 Cores, 1 DBU

### Depedencies

We didn't install any external libraries or dependencies besides the ones already present in the Databricks Community default configs.

There are a few steps to follow:

1. Make sure you have a Databricks Community account.
2. Create a cluster with the configs shown in the **Environment** section. You can create in the menu **Compute** -> **Create compute**.
3. Make sure the **default** database exists on **Catalog** -> **Databases**.
4. Load the [files](https://github.com/winnin/desafio_dataeng/tree/main) ([post_creator.json.gz](https://github.com/winnin/desafio_dataeng/blob/main/posts_creator.json.gz), [wiki_pages.json.gz](https://github.com/winnin/desafio_dataeng/blob/main/wiki_pages.json.gz)) described in the [Instructions](https://github.com/winnin/desafio_dataeng/blob/main/README.md#instru%C3%A7%C3%B5es) section in Databricks. You can follow the instructions written there to load them. To do so, you can use the path **Catalog** -> **Create Table** -> **Drop Files To Upload**.




### Logging

I didn't implemented a proper logging solution for the messages. For now, it will be displayed as prints in the notebook. In the future, it would be nice to implement it.

### Documentation

For each method, there will be a docstring in the Google's docstring format, giving a brief explanation of what that method does, the arguments, what it returns, Notes and an example of usage. Besides that, I placed some type hints for the methods.

There are a few comments in the code, usually giving a hint of why the code was written that way or a decision made.

The Notebooks have a few markdown cells to better guide you throught it.

### Images

There's a folder named `images` that contains some graphs for the Exercise 4. You can do some analysis with them, for example, percentage of likes vs views, users that post more frequently, etc.

### Comments in Markdown cells

Throughout the code, I placed a few cells with some observations, comments or decisions made.

### Tests

Each notebook has a few cells with Integration and Unity Tests.

* **1 - create_table_creators_scrape_wiki**: Contains Data Integrity and Table Integrity checks. If a test fails, an Exception will be raised or a table with "wrong" data will appear. If the test passes, it will have 2 possible outputs:
    1. No message will be shown
    2. A Table or a message showing a positive output will be displayed.  
* **2 - create_table_posts_creator**: Contains Data Integrity and Table Integrity checks. If a test fails, an Exception will be raised or a table with "wrong" data will appear. If the test passes, it will have 2 possible outputs:
    1. No message will be shown (usually asserts);
    2. A Table or a message showing a positive output will be displayed.
* **3 - create_table_user_yt_from_wikipedia_api**: Contains tests for both the success and fail cases, mocking the requests and functions called. If a test fails, an Exception will be raised. If the test passes, a message in the foollowing format will be displayed:
    > (method_name) SUCCEEDED!

    Sometimes the fail cases will display a few messages explaining the problems encountered, but since it's the expected behaviour, it does not make the test fail.
* **4 - analyze_creators**: Contains Unit tests for all transformations. If a test fails, an Exception will be raised. If the test passes, no text will be shown. These tests have the following structure:
    1. A comment explaining what the original method should do. It has this structure:
        >(method_name) should (expected behaviour).
        >
        > Example: get_top_3_liked_posts_by_creator should return the top 3 most videos posts for each creator for last 6 months
    2. Mocked data for the required DataFrames
    3. Expected Schema (`expercted_schema`)
    4. Mocked data of Expected Result after the transformation (`df_expected`)
    5. DataFrame with the mocked data transformed by the original method (`df_result`)
    6. Assertion for DataFrame equality (`assertDataFrameEqual(df_result, df_expected)`)

    The last cell in this Notebook will run all tests.

## Instructions

### Running the Notebooks

Assuming you have Databricks setup done, you can run the Notebooks. They should be executed in the following order:

>**1 - create_table_creators_scrape_wiki** -> **2 - create_table_posts_creator** -> **3 - create_table_user_yt_from_wikipedia_api** -> **4 - analyze_creators**

and you should be able to run each cell in sequence. The order is required since the first 3 notebooks are dependencies for the fouth one. The first and the second one creates the `default.creators_scrape_wiki` and `default.posts_creator` tables, respectively. The third one gets the `user_id` from Wikipedia pages that will be used in `analyze_creators` Notebook too.