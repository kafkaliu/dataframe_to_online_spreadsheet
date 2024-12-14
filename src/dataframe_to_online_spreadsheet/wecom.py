import datetime
import json
import logging
import requests

import numpy as np
import pandas as pd


@pd.api.extensions.register_dataframe_accessor("wecom")
class WecomAccessor:
    def __init__(self, pandas_obj):
        self._validate(pandas_obj)
        self._obj = pandas_obj
        self._client = Client()

    @staticmethod
    def _validate(obj):
        # TODO
        pass

    def to_spreadsheet(self, app_id, app_secret, doc_id, sheet_id, fields_ids, mode="append"):
        r"""
        Converts data to a Wecom smartsheet.

        Attempts to add/update data to an existing sheet based on the provided smartsheet doc id.
        If `record_id` is empty, the new record will be appended. Otherwrise, the existing record will be updated.

        Parameters:
        - app_id: The application id for authentication.
        - app_secret: The application secret for authentication.
        - doc_id: The id of the smartsheet.
        - sheet_id: The id of the sheet of the smartsheet. NOTICE: The schema of the sheet should match the dataframe.
        - fields_ids: A fields list of the sheet.
            You should use `get_fields` to get the fields ids firstly. See also: https://developer.work.weixin.qq.com/document/path/100229
        - mode: The mode of the operation, such as 'append', 'overwrite'. Default is `append`.

        Returns:
        - The new appended records of the smartsheet.
        """

        access_token = self._client.get_access_token(app_id, app_secret)

        if mode == "overwrite":
            self._client.truncate_records(access_token, doc_id, sheet_id)

        if "record_id" in self._obj.columns:
            result_to_be_added = self._obj[pd.isna(self._obj["record_id"])].drop("record_id", axis=1)
            result_to_be_updated = self._obj[pd.notna(self._obj["record_id"])]
        else:
            result_to_be_added = self._obj.copy()
            result_to_be_updated = pd.DataFrame()

        added = self._client.add_records(
            access_token,
            doc_id,
            sheet_id,
            fields_ids,
            result_to_be_added,
        )

        self._client.update_records(
            access_token,
            doc_id,
            sheet_id,
            fields_ids,
            result_to_be_updated,
        )

        return added


class Client(object):
    def __init__(self, host="https://qyapi.weixin.qq.com"):
        self._host = host

    def get_access_token(self, app_id, app_secret):
        r"""
        See also: https://developer.work.weixin.qq.com/document/path/91039
        """

        url = f"{self._host}/cgi-bin/gettoken?corpid={app_id}&corpsecret={app_secret}"
        resp = self._get(url)
        return resp["access_token"]

    def create_doc(self, access_token, title, manager_id):
        r"""
        See also: https://developer.work.weixin.qq.com/document/path/97470
        """

        payload = {"doc_type": 10, "doc_name": title, "admin_users": [manager_id]}
        resp = self._post(
            f"{self._host}/cgi-bin/wedoc/create_doc?access_token={access_token}",
            payload,
        )
        return resp["docid"], resp["url"]

    def add_sheet(self, access_token, doc_id, title):
        r"""
        See also: https://developer.work.weixin.qq.com/document/path/100214
        """

        payload = {
            "docid": doc_id,
            "properties": {
                "title": title,
            },
        }
        resp = self._post(
            f"{self._host}/cgi-bin/wedoc/smartsheet/add_sheet?access_token={access_token}",
            payload,
        )
        return resp["properties"]["sheet_id"]

    def get_fields(self, access_token, doc_id, sheet_id):
        r"""
        See also: https://developer.work.weixin.qq.com/document/path/100229
        """

        payload = {
            "docid": doc_id,
            "sheet_id": sheet_id,
        }
        resp = self._post(
            f"{self._host}/cgi-bin/wedoc/smartsheet/get_fields?access_token={access_token}",
            payload,
        )
        return resp["fields"]

    def add_records(self, access_token, doc_id, sheet_id, fields_ids, df):
        if df.empty:
            return
        payload = self.gen_records_payload(doc_id, sheet_id, fields_ids, df)
        resp = self._post(
            f"{self._host}/cgi-bin/wedoc/smartsheet/add_records?access_token={access_token}",
            payload,
        )
        return resp["records"]

    def truncate_records(self, access_token, doc_id, sheet_id):
        r"""
        Retrieve all the record and delete all the records.
        See also: https://developer.work.weixin.qq.com/document/path/100230, https://developer.work.weixin.qq.com/document/path/100225
        """

        offset = 0
        record_ids = []
        while True:
            payload = {
                "docid": doc_id,
                "sheet_id": sheet_id,
                "offset": offset,
                "limit": 1000
            }
                
            resp = self._post(
                f"{self._host}/cgi-bin/wedoc/smartsheet/get_records?access_token={access_token}",
                payload,
            )
            records = resp["records"]
            record_ids.extend([r["record_id"] for r in records])
            if not resp["has_more"]:
                break
            offset = resp["next"]

        if record_ids:
            payload = {
                "docid": doc_id,
                "sheet_id": sheet_id,
                "record_ids": [rid for rid in record_ids]
            }
            self._post(
                f"{self._host}/cgi-bin/wedoc/smartsheet/delete_records?access_token={access_token}",
                payload,
            )

        return

    def update_records(self, access_token, doc_id, sheet_id, fields_ids, df):
        if df.empty:
            return
        payload = self.gen_records_payload(doc_id, sheet_id, fields_ids, df, False)
        resp = self._post(
            f"{self._host}/cgi-bin/wedoc/smartsheet/update_records?access_token={access_token}",
            payload,
        )
        return resp["records"]

    def gen_records_payload(self, doc_id, sheet_id, fields_ids, df, is_new=True):
        r"""
        Generates the payload for adding or updating records.
        """

        df = df.copy()
        df.columns = (
            fields_ids.keys() if is_new else ["record_id"] + list(fields_ids.keys())
        )
        return {
            "docid": doc_id,
            "sheet_id": sheet_id,
            "key_type": "CELL_VALUE_KEY_TYPE_FIELD_ID",
            "records": [
                self._gen_row_payload(row, fields_ids, is_new)
                for _, row in df.iterrows()
            ],
        }

    def _gen_row_payload(self, row, fields_ids, is_new=True):
        result = {} if is_new else {"record_id": row["record_id"]}
        return {
            **result,
            **{
                "values": {
                    field_id: self._gen_cell_value(fields_ids[field_id], row[field_id])
                    for field_id in fields_ids
                    if pd.notna(row[field_id])
                }
            },
        }

    def _gen_cell_value(self, field_type, cell):
        r"""
        See also: https://developer.work.weixin.qq.com/document/path/100224#value
        """

        if field_type == "FIELD_TYPE_TEXT":
            return [{"type": "text", "text": str(cell)}]
        elif field_type == "FIELD_TYPE_USER":
            return [{"user_id": cell}]
        elif field_type == "FIELD_TYPE_NUMBER":
            return cell
        elif field_type == "FIELD_TYPE_DATE_TIME":
            return (
                f"{int((cell if isinstance(cell, datetime.datetime) else pd.to_datetime(cell)).timestamp() * 1000)}"
                if pd.notna(cell)
                else None
            )
        else:
            raise WecomException(-1, f"Unknown field type: {field_type}")

    def _post(self, url, payload):
        response = requests.post(url, json=payload)
        return self._process_response(response)

    def _get(self, url, payload=None):
        response = requests.get(url, json=payload)
        return self._process_response(response)

    def _process_response(self, response):
        try:
            resp = response.json()
            code = resp["errcode"]
        except json.JSONDecodeError:
            logging.error("Invalid response: " + response.text)
            code = -1

        if code == -1 and response.status_code != 200:
            response.raise_for_status()

        if code != 0:
            raise WecomException(code, resp["errmsg"])
        return resp


class WecomException(Exception):
    def __init__(self, code=0, msg=None):
        self.code = code
        self.msg = msg

    def __str__(self) -> str:
        return "{}:{}".format(self.code, self.msg)

    __repr__ = __str__
