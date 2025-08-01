import requests
import pandas as pd
from lxml import etree
from sqlalchemy import create_engine
import urllib

# پارامترها
username = "novinib.com"
password = "n07!1\\1!13.Com04"
flow = 1

url = "http://service.tsetmc.com/webservice/TsePublicV2.asmx"

headers = {
    "Content-Type": "text/xml; charset=utf-8",
    "SOAPAction": '"http://tsetmc.com/Instrument"'
}

soap_body = f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
               xmlns:xsd="http://www.w3.org/2001/XMLSchema"
               xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <Instrument xmlns="http://tsetmc.com/">
      <UserName>{username}</UserName>
      <Password>{password}</Password>
      <Flow>{flow}</Flow>
    </Instrument>
  </soap:Body>
</soap:Envelope>"""

response = requests.post(url, data=soap_body.encode('utf-8'), headers=headers)
xml_str = response.content

# پارس کردن XML پاسخ
root = etree.fromstring(xml_str)

# نام‌فضاها
ns = {
    'soap': 'http://schemas.xmlsoap.org/soap/envelope/',
    'ns': 'http://tsetmc.com/'
}

# رسیدن به داده‌ها
# مسیر به InstrumentResult (ممکنه بسته به ساختار پاسخ فرق کنه)
instrument_result = root.find('.//soap:Body/ns:InstrumentResponse/ns:InstrumentResult', namespaces=ns)

if instrument_result is None:
    print("داده‌ای پیدا نشد")
    exit()

# داده‌ها معمولا درون یک عنصر xsd:schema قرار داره (XML Schema)
schema = instrument_result.find('.//{http://www.w3.org/2001/XMLSchema}schema')

if schema is None:
    print("Schema پیدا نشد، احتمالا ساختار متفاوت است")
    exit()

# اگر داده‌ها به شکل XML DataSet هستند باید از بخش data استفاده کنیم
# این بخش کمی پیچیده‌ست چون باید داده‌های tabular را از XML استخراج کنیم

# ساده‌ترین کار استخراج همه TseInstruments:
rows = []

for tse_instrument in instrument_result.findall('.//TseInstruments'):
    row = {}
    for elem in tse_instrument:
        tag_name = etree.QName(elem).localname
        row[tag_name] = elem.text
    rows.append(row)

# اگر rows خالی بود، ممکنه نام‌فضاها باعث نشدن داده‌ها پیدا بشن؛
# می‌توانی بجای findall بالا از XPath بدون نام‌فضا استفاده کنی یا namespace را اصلاح کنی.

# ساخت DataFrame
df = pd.DataFrame(rows)

print(df.head())

# اتصال به SQL Server
server = 'localhost'
database = 'test'
username_sql = 'sa'
password_sql = 'Ada@20215'

params = urllib.parse.quote_plus(
    f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username_sql};PWD={password_sql}'
)


# اتصال به SQL Server (اگر قبلا ایجاد نکردی)
params = urllib.parse.quote_plus(
    f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username_sql};PWD={password_sql}'
)
engine = create_engine(f'mssql+pyodbc:///?odbc_connect={params}')

# درج داده در جدول tsetmc_test (اگر جدول از قبل هست داده جدید اضافه می‌شود)
df.to_sql('tsetmc_test', con=engine, if_exists='append', index=False)
print("✅ داده‌ها با موفقیت در جدول 'tsetmc_test' درج شدند.")
