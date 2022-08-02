

from airflow.operators.python import PythonOperator
from bs4 import BeautifulSoup
#from selenium import webdriver
# import chromedriver_binary  # Adds chromedriver binary to path
import requests
# from more_itertools import sliced
import re
import numpy as np
import pandas as pd
import time
import json
# from nltk.tok enize import sent_tokenize
# from nltk import *
from itertools import chain
import sqlalchemy
from sqlalchemy import create_engine
import psycopg2
import time
#from IPython.display import clear_output
from airflow.providers.postgres.hooks.postgres import PostgresHook
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname('access_file.py'),
                                                os.path.pardir)))
import access_file


def return_html_page(nbr = int):
    """
    This function returns html pages for the latest fanfiction
    Input: int for the web page that should be read into
    returns: list of h4 headning so another function can extract fanfiction id
    """
    time.sleep(8)
    nbr += 15 # Make so it is not the first page because than it is only fanfics without comments or kudos
    read_url = "https://archiveofourown.org/works/search?commit=Search&page={}&utf8=%E2%9C%93&work_search%5Bbookmarks_count%5D=&work_search%5Bcharacter_names%5D=&work_search%5Bcomments_count%5D=&work_search%5Bcomplete%5D=&work_search%5Bcreators%5D=&work_search%5Bcrossover%5D=&work_search%5Bfandom_names%5D=&work_search%5Bfreeform_names%5D=&work_search%5Bhits%5D=&work_search%5Bkudos_count%5D=&work_search%5Blanguage_id%5D=&work_search%5Bquery%5D=&work_search%5Brating_ids%5D=&work_search%5Brelationship_names%5D=&work_search%5Brevised_at%5D=&work_search%5Bsingle_chapter%5D=0&work_search%5Bsort_column%5D=created_at&work_search%5Bsort_direction%5D=desc&work_search%5Btitle%5D=&work_search%5Bword_count%5D=".format(nbr)
    url = requests.get(read_url)
    html_soup = BeautifulSoup(url.content, 'html.parser')
    h4_soup = html_soup.findAll("h4", class_="heading")
    return h4_soup


def get_fanfic_ids(html_page):
    """
    Input: list of h4 heading that contains ids for the fanfictions

    return: 20 ids for the current web page
    """
    id_list = []
    for i in range(1, 21):
        text = str(html_page[i])
        first_part = text[30:].partition('">')[0]
        number = re.sub("[^0-9]", "", first_part)
        id_list.append(number)
    return id_list



def collect_all_ids(nbr):
    """
    This serves as the main function concerning collecting all the fanfiction ids
    input: nbr of pages to go down the webpage, one page is 20 fanfictions
    returns: list of fanfiction ids
    """
    ids_list = []
    for i in range(1, nbr+1):
        soup = return_html_page(i)
        ids_list.append(get_fanfic_ids(soup))
    return list(chain(*ids_list))  #unlist a list of lists


def insert_metric_values(fanfic_id, link, text, html_soup, dic):
    """
    Function to insert the metrics into a dic
    Inputs: fanfic_id, link and text are. The rest of the values are created in the function
    returns: dic with updated values --> it does not return the dic in just changes it
    """

    metric_dic = {'dd': ['rating', 'category', 'fandom', 'relationship',
                         'character', 'language', 'published', 'words',
                         'chapters', 'comments', 'kudos', 'bookmarks',
                         'hits'],
                  'h2': ['title heading']}

    dic['id'].append(fanfic_id)
    dic['link'].append(link)
    dic['text'].append(text)
    dic["author"].append(html_soup.find("h3", class_="byline heading").get_text().strip())

    for key in metric_dic:
        for sak in (metric_dic[key]):
            try:
                dic[sak].append(html_soup.find(key, class_=sak).get_text().strip())
            except:
                if sak == 'comments' or sak == 'kudos' or sak == 'bookmarks' or sak == 'hits':
                    dic[sak].append(0)
                else:
                    dic[sak].append(float("NaN"))



# function to read data from the whole fanfiction
def read_fanfictions(fanfic_list):
    """
    Keyword arguments:
    argument: A list of fanfiction ids
    Return: dic of fanfictions and data in a flat manner good for dataframes
    """

    fanfic_dic = {'id': [], 'link': [], 'text': [], 'link': [], 'rating': [],
                  'category': [], 'fandom': [], 'relationship': [],
                  'character': [], 'language': [], 'published': [],
                  'words': [], 'chapters': [], 'comments': [], 'kudos': [],
                  'bookmarks': [], 'hits': [], 'title heading': [],
                  'author': []}

    for i in range(len(fanfic_list)):
        fanfic_id = fanfic_list[i]
        fanfiction = "https://archiveofourown.org/works/{}?view_adult=true".format(fanfic_id)
        time.sleep(10)
        url = requests.get(fanfiction)
        html_soup = BeautifulSoup(url.content, 'html.parser')  # get the html soup
        try:
            text = html_soup.find("div", class_="userstuff").get_text(separator="\n")
        except:
            print(f"A error occurred for: {fanfiction} and the url status is:  {url.status_code}")
            text = ""

        insert_metric_values(fanfic_id, fanfiction, text, html_soup, fanfic_dic)

    return fanfic_dic


def postgres_engine():  # for local database
    postgres_str = 'postgresql://{username}:{password}@{ipaddress}:{port}/{db_name}'.format(
        username= access_file.username,
        password= access_file.password,
        ipaddress= access_file.ipaddress,
        port=access_file.port,
        db_name=access_file.db_name,)

    cnx = create_engine(postgres_str)
    return cnx


def airflow_engine(): #for airflow engine to
    hook = PostgresHook(postgres_conn_id=access_file.postgres_conn_id )  #to airflow
    conn = hook.get_conn()  # this returns psycopg2.connect() object
    airflow_engine = hook.get_sqlalchemy_engine()
    return airflow_engine



def fanfiction_to_database(ds, nbr = 2, **kwargs):
  fanfic_list = collect_all_ids(nbr)  # the number of pages we will look at, each page consist of 20 ids/fanfictions
  dic_of_fanfics = read_fanfictions(fanfic_list)
  df = pd.DataFrame.from_dict(dic_of_fanfics)
  df['download_date'] = ds  #get the date when airflow downloaded the data
  df.to_sql('fanfictions', con = airflow_engine(), if_exists = 'append', index = False)
  print("Its done, it has been saved to the database")

def fanfiction_to_dataframe(nbr = 2):
  fanfic_list = collect_all_ids(nbr)  # the number of pages we will look at, each page consist of 20 ids/fanfictions
  dic_of_fanfics = read_fanfictions(fanfic_list)
  df = pd.DataFrame.from_dict(dic_of_fanfics)
  return df



OPERATOR_PARAMS = dict()


def get_insert_metric(sql, dest_table, source_conn_id, dest_conn_id, **context):
    source = PostgresHook(postgres_conn_id=source_conn_id)  # The table getting data from
    dest = PostgresHook(postgres_conn_id=dest_conn_id)  # The destination table
    source_conn = source.get_conn()
    cursor = source_conn.cursor()
    cursor.execute(sql)
    dest.insert_rows(table=dest_table, rows=cursor)
    cursor.close()
    source_conn.close()


def metric_operator(metric, dest_table, source_conn_id, dest_conn_id, task_id = None):

    # Params is used for getting variables inside a sql query
    params = {
        **OPERATOR_PARAMS,
        **{
            'metric': metric # Make so sql will get the metric
        }
    }

    return PythonOperator(
        task_id = metric if task_id is None else task_id,
        python_callable= get_insert_metric,
        params = params,
        op_kwargs={'sql': f'sql/select_{metric}.sql',
                   'dest_table': dest_table,
                   'source_conn_id': source_conn_id,
                   'dest_conn_id':dest_conn_id
        },
        templates_exts=['.sql']

    )

