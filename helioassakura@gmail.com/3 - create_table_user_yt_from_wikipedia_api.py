# Databricks notebook source
import requests
import pandas as pd
from pyspark import pandas
import json
import re

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Definitions

# COMMAND ----------

YOUTUBE_USER_REGEX = r'^(https?:\/\/(www\.)?youtube\.com\/user\/([a-zA-Z\-_0-9]+))\/?$'
WIKI_ENDPOINT = "https://en.wikipedia.org/w/api.php"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC > **Comment**
# MAGIC >
# MAGIC > The regex in YOUTUBE_USER_REGEX is supposed to get the following links:
# MAGIC >
# MAGIC > 1. https://youtube.com/user/a_user
# MAGIC > 2. http://www.youtube.com/user/another_user
# MAGIC > 3. http://youtube.com/user/another_user2
# MAGIC > 4. http://youtube.com/user/another_user-/
# MAGIC > 5. And a few more. You can check in the regex101 website a detailed explanation of this regex
# MAGIC >
# MAGIC > Sometimes there's a @ in the link, but we are not considering this case. 

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Scraper

# COMMAND ----------

df_page_names = spark.read.table("default.creators_scrape_wiki")
pd_wiki_pages = df_page_names.toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC > **Comment**
# MAGIC >
# MAGIC > Performance should not be a very big problem if we use Pandas, since the json sizes are not big and we don't need to perform a lot of transformations with the data

# COMMAND ----------

display(df_page_names)

# COMMAND ----------

pd_wiki_pages

# COMMAND ----------

"""
    Retrieve wiki page from the provided endpoint using the given parameters

    Args:
        params (dict): Dictionary containing parameters to be passed in the request
        endpoint (str): The URL endpoint for the request

    Returns:
        list or None: A list of wiki page data if the request is successful (status code 200), otherwise None

    Raises:
        Exception: An error occurred during the execution

    Note:
        This function sends a GET request to the specified endpoint with the provided parameters. If the request is successful, it returns the parsed JSON response. If an error occurs during the request, the exception is printed, and None is returned

    Example:
        >>> params = {"action": "parse", "format": "json", "page": "a_page_name"}
        >>> endpoint = 'https://en.wikipedia.org/w/api.php'
        >>> get_wiki_page_from_wiki_names(params, endpoint)
"""

def get_wiki_page_from_wiki_names(params: dict, endpoint: str) -> list:
    try:
        response = requests.get(endpoint, params = params)
        if response.status_code == 200:
            return json.loads(response.text)
        return None
    except Exception as e:
        print(e)
        raise

# COMMAND ----------

"""
    Extracts user ID from the Wikipedia API response

    Args:
        json_response (dict): The JSON response received from the Wikipedia API

    Returns:
        str or None: The user ID extracted from the response if found, otherwise None

    Raises:
        Exception: An error occurred during the execution

    Note:
        This function attempts to extract the user ID from the provided Wikipedia API response. It looks for YouTube user links in the 'externallinks'. If multiple YouTube user links are found, it ignores them and returns None. If an error occurs during the extraction process, the exception is printed, and None is returned
        It has the following priorities to get the user name:
            1. Checks if there's a REGEX match in a link inside externallinks
            3. There's no link to get the user_id
        
        REGEX to match: ^(https?:\/\/(www\.)?youtube\.com\/user\/([a-zA-Z\-_0-9]+))\/?$
            It checks links such as:
                - https://www.youtube.com/user/an_example -> an_example
                - https://youtube.com/user/another_example_123/ -> another_example_123

    Example:
        >>> wiki_json_response = {...}  # JSON response from Wikipedia API
        >>> get_user_id_from_wiki_api_response(wiki_json_response)
"""

def get_user_id_from_wiki_api_response(json_response: dict) -> str:
    user_ids = None
    try:
        youtube_users = None
        if "parse" in json_response.keys():
            # The Wikipedia page has a Youtube User external link
            if "externallinks" in json_response["parse"].keys():
                r = re.compile(YOUTUBE_USER_REGEX)
                youtube_users = set(filter(r.match, json_response["parse"]["externallinks"]))
            else:
                return None
        if youtube_users:
            # There might be more than one link to an Youtube User page. We will desconsider this page.
            if len(youtube_users) > 1:
                print("Too many users in request, shutting down...")
                return None
            user_ids = [x.split("/")[-1] for x in youtube_users][0]
    except Exception as e:
        print (e)
    return user_ids

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC > **Comment**
# MAGIC >
# MAGIC > Maybe getting the user_id from Wikipedia page is not the best way. There might be another way to do it. Besides that, we are assuming the following premises:
# MAGIC >
# MAGIC > 1. There is a link in the "externallinks" that will give us the Youtuber's user_id
# MAGIC > 2. There is only one link with an unique Youtuber user_id. If there's more, we will disconsider all of them, since there's no "easy" way to determine which is the right one
# MAGIC > 3. The link matches the YOUTUBE_USER_REGEX
# MAGIC
# MAGIC Obs:
# MAGIC > There is another possible way to get the link for the Youtube channel and user_id, but it's from the entire HTML written in the JSON response. We are assuming that this link is in the "externallinks"
# MAGIC
# MAGIC > **Another Comment**
# MAGIC >
# MAGIC > We are not parallelizing the requests, not requiring to create threads or something like that. Since we are only running it for ~10 users, it will not take too much time

# COMMAND ----------

"""
    Create a dictionary mapping Wikipedia page names to YouTube user IDs

    Args:
        df_wiki_pages (pd.DataFrame): DataFrame containing Wikipedia page names

    Returns:
        dict: A dictionary where keys are YouTube user IDs and values are corresponding Wikipedia page names

    Note:
        This function iterates over the provided DataFrame of Wikipedia page names and retrieves YouTube user IDs for each page using the Wikipedia API. It constructs a dictionary where each key is a YouTube user ID and each value is the corresponding Wikipedia page name. If an error occurs during the retrieval process for a specific page, the exception is caught, and a message is printed

    Example:
        >>> wiki_pages_df = pd.DataFrame({'wiki_page': ['Page1', 'Page2']})
        >>> create_yt_users_dict(wiki_pages_df)
"""

def create_yt_users_dict(df_wiki_pages: pd.DataFrame) -> dict:

    params = {"action": "parse", "format": "json"}
    dict_users_yt = {'user_id': [], 'wiki_page': []}

    for page_name in df_wiki_pages['wiki_page']:
        try:
            dict_users_yt['wiki_page'].append(page_name)
            params['page'] = f"{page_name}"
            wiki_response = get_wiki_page_from_wiki_names(params, WIKI_ENDPOINT)
            user_id = get_user_id_from_wiki_api_response(wiki_response)
            dict_users_yt['user_id'].append(user_id)
        except Exception as e:
            print (f"Coulnd't get user_id from {page_name}", e)
    
    return dict_users_yt

# COMMAND ----------

df_users_yt = pd.DataFrame.from_dict(create_yt_users_dict(pd_wiki_pages))
spark.createDataFrame(df_users_yt).write.mode("overwrite").format("delta").saveAsTable("default.users_yt")

# COMMAND ----------

display(spark.read.table("default.users_yt"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Tests

# COMMAND ----------

import requests
import json
from unittest.mock import patch

# COMMAND ----------

# get_wiki_page_from_wiki_names should return the response from the Wikipedia API as json

def test_get_wiki_page_from_wiki_names_success():
    print("Running test_get_wiki_page_from_wiki_names_success...")
    with patch.object(requests, 'get') as mock_get:
        mock_response = {"parse": {"externallinks": ["https://youtube.com/user/an_example"]}}
        mock_get.return_value.status_code = 200
        mock_get.return_value.text = json.dumps(mock_response)

        result = get_wiki_page_from_wiki_names({"action": "parse", "format": "json", "page": "an_page"}, "https://mockapi.com")

        assert result == mock_response
        print("test_get_wiki_page_from_wiki_names_success SUCCEEDED!\n")

def test_get_wiki_page_from_wiki_names_bad_request():
    print("Running test_get_wiki_page_from_wiki_names_bad_request...")
    with patch.object(requests, 'get') as mock_get:
        # Bad Request
        mock_get.return_value.status_code = 400

        result = get_wiki_page_from_wiki_names({"action": "parse", "format": "json", "page": "an_page"}, "https://mockapi.com")

        assert result is None
        print("test_get_wiki_page_from_wiki_names_bad_request SUCCEEDED!\n")

test_get_wiki_page_from_wiki_names_success()
test_get_wiki_page_from_wiki_names_bad_request()

# COMMAND ----------

# get_user_id_from_wiki_api_response should return the user_id extracted from a Wikipedia API json.

def test_get_user_id_from_wiki_api_response_with_valid_json():
    print("Running test_get_user_id_from_wiki_api_response_with_valid_json...")
    json_response = {
        "parse": {
            "externallinks": ["https://youtube.com/user/a_user"]
        }
    }
    result = get_user_id_from_wiki_api_response(json_response)
    assert result == "a_user"
    print("test_get_user_id_from_wiki_api_response_with_valid_json SUCCEEDED!\n")

def test_get_multiple_user_ids():
    print("Running test_get_multiple_user_ids...")
    json_response = {
        "parse": {
            "externallinks": ["https://youtube.com/user/a_user", "http://youtube.com/user/another_user"]
        }
    }
    result = get_user_id_from_wiki_api_response(json_response)
    assert result is None
    print("test_get_multiple_user_ids SUCCEEDED!\n")

def test_get_user_id_from_wiki_api_response_with_invalid_json():
    print("Running test_get_user_id_from_wiki_api_response_with_invalid_json...")
    json_response = {
        "not_externallinks": "not_a_valid_value"
    }
    result = get_user_id_from_wiki_api_response(json_response)
    assert result is None
    print("test_get_user_id_from_wiki_api_response_with_invalid_json SUCCEEDED!\n")

def test_get_user_id_from_wiki_api_response_with_no_external_links():
    print("Running test_get_user_id_from_wiki_api_response_with_no_external_links...")
    json_response = {
        "parse": {
            "not_externallinks": "not_a_valid_value"
        }
    }
    result = get_user_id_from_wiki_api_response(json_response)
    assert result is None
    print("test_get_user_id_from_wiki_api_response_with_no_external_links SUCCEEDED!\n")

test_get_user_id_from_wiki_api_response_with_valid_json()
test_get_multiple_user_ids()
test_get_user_id_from_wiki_api_response_with_invalid_json()
test_get_user_id_from_wiki_api_response_with_no_external_links()

# COMMAND ----------

# create_yt_users_dict should return a table with the user_ids of all wiki_pages

def test_create_yt_users_dict_with_valid_data():

    print("Running test_create_yt_users_dict_with_valid_data...")
    df_wiki_pages = pd.DataFrame({'wiki_page': ['first_page', 'another_page']})

    with patch('__main__.get_wiki_page_from_wiki_names') as mock_get_wiki_page:
        mock_get_wiki_page.return_value = {"parse": {"externallinks": ["https://youtube.com/user/a_user"]}}

        with patch('__main__.get_user_id_from_wiki_api_response') as mock_get_user_id:
            mock_get_user_id.return_value = "a_user"
            result = create_yt_users_dict(df_wiki_pages)

            
            assert result == {'user_id': ['a_user', 'a_user'], 'wiki_page': ['first_page', 'another_page']}
            print("test_create_yt_users_dict_with_valid_data SUCCEEDED!\n")

def test_create_yt_users_dict_with_invalid_data():

    print("Running test_create_yt_users_dict_with_invalid_data...")
    df_wiki_pages = pd.DataFrame({'wiki_page': ['first_page', 'another_page']})

    with patch('__main__.get_wiki_page_from_wiki_names') as mock_get_wiki_page:
        mock_get_wiki_page.side_effect = Exception("Mock Exception")
        result = create_yt_users_dict(df_wiki_pages)

        
        assert result == {'user_id': [], 'wiki_page': ['first_page', 'another_page']}
        print("test_create_yt_users_dict_with_invalid_data SUCCEEDED!\n")

test_create_yt_users_dict_with_valid_data()
test_create_yt_users_dict_with_invalid_data()