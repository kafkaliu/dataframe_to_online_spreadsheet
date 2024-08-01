# Export a pandas dataframe to an online spreadsheet

The online spreadsheet such as Google Sheets, Feishu Docs, is a good way to share data with others and is a powerful tool for data analysis and visualization. This package provides a simple way to export a pandas dataframe to a online spreadsheet.

## Installation
You can install this package from PyPI using pip:
```bash
pip install dataframe-to-online-spreadsheet
```

## Usage

### Feishu Docs

1. You need to register a Feishu team and get the manager_ids from [Feishu Admin](https://www.feishu.cn/). Fortunately, it is free for a small team.
2. You need to create an app and get the app_id and app_secret from [Feishu Open Platform](https://open.feishu.cn/).

```python
import pandas as pd
import dataframe_to_online_spreadsheet.feishu
import os

def test_feishu():

    app_id = os.getenv("APP_ID")
    app_secret = os.getenv("APP_SECRET")

    sheet1_data = pd.read_csv("./tests/test_data1.csv")
    token = sheet1_data.feishu.to_spreadsheet(app_id, app_secret, title="Daily Report", data={'title': 'sheet_name1', 'data': sheet1_data}, manager_ids=['xxx'])

    sheet2_data = pd.read_csv("./tests/test_data2.csv")
    token = sheet2_data.feishu.to_spreadsheet(app_id, app_secret, title="Daily Report", data={'title': 'sheet_name2', 'data': sheet2_data}, manager_ids=['xxx'], spreadsheet_token=token)

    sheet3_data = pd.read_csv("./tests/test_data3.csv")
    token = sheet3_data.feishu.to_spreadsheet(app_id, app_secret, title="Daily Report", data={'title': 'sheet_name3', 'data': sheet3_data}, manager_ids=['xxx'], spreadsheet_token=token)
    logging.info(f"spreadsheet token: {token}")
    assert token