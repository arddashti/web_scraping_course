import requests
import pandas as pd
from sqlalchemy import create_engine
import urllib

# دریافت داده از API
url = 'https://cdn.tsetmc.com/api/ClosingPrice/GetTradeTop/MostVisited/1/7'
response = requests.get(url).json()

# تبدیل به DataFrame با باز کردن فیلدهای تو در تو
items = response["tradeTop"]
df = pd.json_normalize(items)

# اتصال به SQL Server
server = 'localhost'
database = 'test'
username = 'sa'
password = 'Ada@20215'

# ایجاد رشته اتصال
params = urllib.parse.quote_plus(
    f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'
)
engine = create_engine(f'mssql+pyodbc:///?odbc_connect={params}')

# درج داده در جدول tsetmc_test (در صورت وجود، جایگزین می‌شود)
df.to_sql('tsetmc_test', con=engine, if_exists='replace', index=False)

print("✅ داده‌ها با موفقیت در جدول 'tsetmc_test' درج شدند.")
