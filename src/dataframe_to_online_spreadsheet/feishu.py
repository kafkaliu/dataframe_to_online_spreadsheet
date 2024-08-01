import json
import logging
import requests

import numpy as np
import pandas as pd


@pd.api.extensions.register_dataframe_accessor("feishu")
class FeishuAccessor:
    def __init__(self, pandas_obj):
        self._validate(pandas_obj)
        self._obj = pandas_obj
        self._client = Client("https://open.feishu.cn")

    @staticmethod
    def _validate(obj):
        # TODO
        pass

    def to_spreadsheet(self, app_id, app_secret, title, sheet_title, manager_ids, spreadsheet_token=None):
        r"""
        Converts data to a Feishu spreadsheet.

        Attempts to create a new spreadsheet or add data to an existing one based on the provided spreadsheet token. 
        If the spreadsheet already contains a worksheet with the same title as the data, it will delete the old worksheet before adding new data.

        Parameters:
        - app_id: The application ID for authentication.
        - app_secret: The application secret for authentication.
        - data: The data to be converted into the spreadsheet.
        - title: The title of the spreadsheet.
        - manager_ids: A list of manager IDs to whom permissions will be granted for the spreadsheet.
        - spreadsheet_token: An existing spreadsheet token, if provided, will attempt to add data to this spreadsheet rather than creating a new one.

        Returns:
        - The token of the spreadsheet after conversion.
        """

        access_token = self._client.get_access_token(app_id, app_secret)

        # Create a new spreadsheet or reuse an existing one based on whether a spreadsheet token is provided
        if spreadsheet_token is None:
            token, _ = self._client.create_spreadsheet(access_token, title)
        else:
            token, _ = spreadsheet_token, None
            # Check if the spreadsheet already has a worksheet with the same title as the data
            worksheets = self._client.list_worksheets(access_token, token)
            # If a matching worksheet is found, delete it
            sheet_id = next((sheet["sheet_id"] for sheet in worksheets if sheet["title"] == sheet_title), None)
            if sheet_id:
                self._client.delete_worksheet(access_token, token, sheet_id)

        # Create a new worksheet in the spreadsheet
        sheet_id = self._client.create_worksheet(access_token, token, sheet_title)

        # Grant "full_access" permissions to each user in the manager_ids list
        for manager_id in manager_ids:
            self._client.add_permissions_member(
                access_token, token, manager_id, "full_access"
            )

        # Batch update data into the spreadsheet
        self._batch_update(access_token, token, sheet_id)

        # Return the spreadsheet token
        return token

    def _batch_update(self, access_token, doc_token, sheet_id):
        r"""
        Batch updates data to a Feishu spreadsheet.

        This method uploads DataFrame content to a Feishu spreadsheet in batches.
        It first uploads the header row, then iterates through the DataFrame in chunks to upload the data rows.

        Parameters:
        - access_token: The access token for authorization.
        - doc_token: The document token identifying the specific document.
        - sheet_id: The ID of the sheet to be updated.
        """

        # Define the header range of the spreadsheet in the format "sheet_id!A1:Z1", where Z1 represents the column ID of the last column.
        header_range = f"{sheet_id}!A1:{self._spreadsheet_column_id(self._obj.shape[1])}1"
        data = {"valueRanges": [{"range": header_range, "values": [self._obj.columns.to_list()]}]}

        # Send the request to update the header.
        self._client.batch_update_values(access_token, doc_token, data)

        # Define the maximum number of rows per batch. See also: https://open.feishu.cn/document/server-docs/docs/sheets-v3/data-operation/write-data-to-multiple-ranges?lang=en-US
        max_size = 5000

        # Upload the data rows in batches.
        for i in range(0, self._obj.shape[0], max_size):
            df = self._obj.iloc[i : i + max_size]
            # Calculate the range for the current batch of data.
            body_range = f"{sheet_id}!A{i + 2}:{self._spreadsheet_column_id(len(self._obj.columns))}{df.shape[0] + i + 1}"
            data = {
                "valueRanges": [
                    {
                        "range": body_range,
                        "values": json.loads(df.to_json(orient='values', date_format='iso', date_unit='s'))
                    }
                ]
            }

            # Send the request to update the data rows.
            self._client.batch_update_values(access_token, doc_token, data)

    def _spreadsheet_column_id(self, col):
        r"""
        Converts a column number into its corresponding Excel column identifier.

        Parameters:
        - col: The column number, starting from 1.

        Returns:
        - The corresponding Excel column identifier.
        """

        result = ""
        i = col
        while i > 0:
            m = int((i - 1) % 26)
            i = int((i - 1) / 26)
            result = chr(m + 65) + result
        return result


class Client(object):
    def __init__(self, host):
        self._host = host

    def get_access_token(self, app_id, app_secret):
        r"""
        See also: https://open.feishu.cn/document/server-docs/authentication-management/access-token/tenant_access_token_internal?lang=en-US
        """

        url = f"{self._host}/open-apis/auth/v3/tenant_access_token/internal"
        headers = {"Content-Type": "application/json; charset=utf-8"}
        payload = {"app_id": app_id, "app_secret": app_secret}
        resp = self._post(url, headers, payload)
        return resp["tenant_access_token"]

    def create_spreadsheet(self, access_token, title, folder_token=None):
        r""" """

        url = f"{self._host}/open-apis/sheets/v3/spreadsheets"
        headers = self._build_headers(access_token)
        payload = {"title": title, "folder_token": folder_token}
        resp = self._post(url, headers, payload)
        return (
            resp["data"]["spreadsheet"]["spreadsheet_token"],
            resp["data"]["spreadsheet"]["url"],
        )

    def create_worksheet(self, access_token, spreadsheet_token, title):
        url = f"{self._host}/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/sheets_batch_update"
        headers = self._build_headers(access_token)
        payload = {"requests": [{"addSheet": {"properties": {"title": title}}}]}
        resp = self._post(url, headers, payload)
        return resp["data"]["replies"][0]["addSheet"]["properties"]["sheetId"]

    def list_worksheets(self, access_token, spreadsheet_token):
        url = f"{self._host}/open-apis/sheets/v3/spreadsheets/{spreadsheet_token}/sheets/query"
        headers = self._build_headers(access_token)
        payload = {}
        resp = self._get(url, headers, payload)
        return resp["data"]["sheets"]

    def delete_worksheet(self, access_token, spreadsheet_token, sheet_id):
        url = f"{self._host}/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/sheets_batch_update"
        headers = self._build_headers(access_token)
        payload = {"requests": [{"deleteSheet": {"sheetId": sheet_id}}]}
        resp = self._post(url, headers, payload)
        return resp["data"]["replies"][0]["deleteSheet"]["sheetId"]

    def batch_update_values(self, access_token, doc_token, data):
        url = f"{self._host}/open-apis/sheets/v2/spreadsheets/{doc_token}/values_batch_update"
        headers = self._build_headers(access_token)
        resp = self._post(url, headers, data)
        return resp["data"]["spreadsheetToken"]

    def add_permissions_member(self, access_token, doc_token, member_id, perm):
        url = f"{self._host}/open-apis/drive/v1/permissions/{doc_token}/members?type=sheet&need_notification=false"
        headers = self._build_headers(access_token)
        payload = {"member_type": "userid", "member_id": member_id, "perm": perm}
        self._post(url, headers, payload)

    def _build_headers(self, access_token):
        return {
            "Content-Type": "application/json; charset=utf-8",
            "Authorization": f"Bearer {access_token}",
        }

    def _post(self, url, headers, payload):
        response = requests.post(url, headers=headers, json=payload)
        return self._process_response(response)

    def _get(self, url, headers, payload):
        response = requests.get(url, headers=headers, json=payload)
        return self._process_response(response)

    def _process_response(self, response):
        try:
            resp = response.json()
            code = resp["code"]
        except json.JSONDecodeError:
            logging.error("Invalid response: " + response.text)
            code = -1

        if code == -1 and response.status_code != 200:
            response.raise_for_status()

        if code != 0:
            raise FeishuException(code, resp["msg"])
        return resp


class FeishuException(Exception):
    def __init__(self, code=0, msg=None):
        self.code = code
        self.msg = msg

    def __str__(self) -> str:
        return "{}:{}".format(self.code, self.msg)

    __repr__ = __str__
