import pytest
import logging
from dotenv import load_dotenv, find_dotenv

import pandas as pd

import os
import sys

import src.dataframe_to_online_spreadsheet.feishu as feishu

load_dotenv("feishu.env")

LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
logging.basicConfig(format=LOG_FORMAT, level=logging.INFO,  handlers=[logging.StreamHandler(sys.stdout)])

def test_feishu():

    app_id = os.getenv("APP_ID")
    app_secret = os.getenv("APP_SECRET")

    sheet1_data = pd.read_csv("./tests/test_data1.csv")
    token = sheet1_data.feishu.to_spreadsheet(app_id, app_secret, title="Daily Report", sheet_title='sheet_name1', manager_ids=['7a7ceg17'])

    sheet2_data = pd.read_csv("./tests/test_data2.csv")
    token = sheet2_data.feishu.to_spreadsheet(app_id, app_secret, title="Daily Report", sheet_title='sheet_name2', manager_ids=['7a7ceg17'], spreadsheet_token=token)

    sheet3_data = pd.read_csv("./tests/test_data3.csv")
    token = sheet3_data.feishu.to_spreadsheet(app_id, app_secret, title="Daily Report", sheet_title='sheet_name3', manager_ids=['7a7ceg17'], spreadsheet_token=token)
    logging.info(f"spreadsheet token: {token}")
    assert token

def test_bug_timestamp_to_json():
    app_id = os.getenv("APP_ID")
    app_secret = os.getenv("APP_SECRET")
    data = {
        'Id': [1, 2, 3],
        'Name': ['ProdA', 'ProdB', 'ProdC'],
        'Date1': pd.to_datetime(['2018-05-18 04:45:08', '2018-05-18 02:15:00', '2018-05-16 10:20:00']),
        'Date2': [pd.Timestamp('2023-10-01 12:30:45'), pd.NaT, pd.Timestamp('2023-10-03 16:00:00')],
        'Value': [10, 20, 30]
    }
    df = pd.DataFrame(data)
    df.feishu.to_spreadsheet(app_id, app_secret, title="Daily Report", sheet_title='sheet_name4', manager_ids=['7a7ceg17'])
    assert True

def test_online_spreadsheet_client():

    app_id = os.getenv("APP_ID")
    app_secret = os.getenv("APP_SECRET")

    client = feishu.Client("https://open.feishu.cn")
    access_token = client.get_access_token(app_id, app_secret)
    token, _ = client.create_spreadsheet(access_token, "test")
    logging.info(f"spreadsheet token: {token}")
    assert token != None

    sheet_id = client.create_worksheet(access_token, token, "sheet one")
    logging.info(f"sheet id: {sheet_id}")
    assert sheet_id

    sheet_id = client.delete_worksheet(access_token, token, sheet_id)
    assert sheet_id
