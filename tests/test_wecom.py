from datetime import datetime
import pytest
import logging
from dotenv import load_dotenv, find_dotenv

import pandas as pd

import os
import sys

import src.dataframe_to_online_spreadsheet.wecom as wecom

load_dotenv("wecom.env")

LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
logging.basicConfig(
    format=LOG_FORMAT, level=logging.INFO, handlers=[logging.StreamHandler(sys.stdout)]
)


def test_wecom():

    app_id = os.getenv("APP_ID")
    app_secret = os.getenv("APP_SECRET")
    doc_id = os.getenv("DOC_ID")
    sheet_id = os.getenv("SHEET_ID")

    sheet1_data = pd.read_csv("./tests/test_data1_wecom2.csv")

    fields_ids = {
        "f3jXlv": "FIELD_TYPE_NUMBER",
        "f8BRn6": "FIELD_TYPE_TEXT",
        "fBgV4J": "FIELD_TYPE_TEXT",
        "fD1c0G": "FIELD_TYPE_TEXT",
        "fGJwzi": "FIELD_TYPE_TEXT",
        "fMtBZy": "FIELD_TYPE_TEXT",
        "fQONOH": "FIELD_TYPE_TEXT",
        "fQQyQY": "FIELD_TYPE_DATE_TIME",
        "fRq1I2": "FIELD_TYPE_DATE_TIME",
        "fSmqgA": "FIELD_TYPE_TEXT",
        "fSu8nr": "FIELD_TYPE_NUMBER",
        "fWY1jO": "FIELD_TYPE_TEXT",
        "fYrjfz": "FIELD_TYPE_NUMBER",
        "fZH5aH": "FIELD_TYPE_TEXT",
        "fZZ5uE": "FIELD_TYPE_NUMBER",
        "fZilZm": "FIELD_TYPE_TEXT",
        "faGRO9": "FIELD_TYPE_TEXT",
        "fcIPid": "FIELD_TYPE_TEXT",
        "ffPlXC": "FIELD_TYPE_TEXT",
        "fgxTYC": "FIELD_TYPE_TEXT",
        "fpqHBM": "FIELD_TYPE_NUMBER",
        "fwLOij": "FIELD_TYPE_TEXT",
        "fxRfyy": "FIELD_TYPE_NUMBER",
        "fzNsma": "FIELD_TYPE_TEXT",
        "fwF1k7": "FIELD_TYPE_USER",
        "fu0X00": "FIELD_TYPE_NUMBER"
    }
    records = sheet1_data.wecom.to_spreadsheet(
        app_id, app_secret, doc_id, sheet_id, fields_ids
    )

    logging.info(records)
    assert records


def test_client_get_fields():
    app_id = os.getenv("APP_ID")
    app_secret = os.getenv("APP_SECRET")
    doc_id = os.getenv("DOC_ID")
    sheet_id = os.getenv("SHEET_ID")

    client = wecom.Client()
    access_token = client.get_access_token(app_id, app_secret)
    fields = client.get_fields(access_token, doc_id, sheet_id)
    assert fields


def test_gen_add_records_payload():
    doc_id = os.getenv("DOC_ID")                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
    # sheet_id = os.getenv("SHEET_ID")
    sheet_id = "Z0adAv"  # sheet1
    fields_ids = {
        "f3jXlv": "FIELD_TYPE_NUMBER",
        "f8BRn6": "FIELD_TYPE_TEXT",
        "fBgV4J": "FIELD_TYPE_TEXT",
        "fD1c0G": "FIELD_TYPE_TEXT",
        "fGJwzi": "FIELD_TYPE_TEXT",
        "fMtBZy": "FIELD_TYPE_TEXT",
        "fQONOH": "FIELD_TYPE_TEXT",
        "fQQyQY": "FIELD_TYPE_DATE_TIME",
        "fRq1I2": "FIELD_TYPE_DATE_TIME",
        "fSmqgA": "FIELD_TYPE_TEXT",
        "fSu8nr": "FIELD_TYPE_NUMBER",
        "fWY1jO": "FIELD_TYPE_TEXT",
        "fYrjfz": "FIELD_TYPE_NUMBER",
        "fZH5aH": "FIELD_TYPE_TEXT",
        "fZZ5uE": "FIELD_TYPE_NUMBER",
        "fZilZm": "FIELD_TYPE_TEXT",
        "faGRO9": "FIELD_TYPE_TEXT",
        "fcIPid": "FIELD_TYPE_TEXT",
        "ffPlXC": "FIELD_TYPE_NUMBER",
        "fgxTYC": "FIELD_TYPE_TEXT",
        "fpqHBM": "FIELD_TYPE_NUMBER",
        "fwLOij": "FIELD_TYPE_TEXT",
        "fxRfyy": "FIELD_TYPE_NUMBER",
        "fzNsma": "FIELD_TYPE_TEXT",
        "fwF1k7": "FIELD_TYPE_USER",
        "fu0X00": "FIELD_TYPE_NUMBER"
    }
    df = pd.read_csv("./tests/test_data1_wecom.csv")
    df = df[pd.isna(df["record_id"])]

    client = wecom.Client()
    payload = client.gen_records_payload(doc_id, sheet_id, fields_ids, df)
    assert "docid" in payload
    assert "sheet_id" in payload
    assert "records" in payload

