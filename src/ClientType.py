import requests
import pandas as pd
from lxml import etree
from sqlalchemy import text
from config import engine, TSETMC_USERNAME, TSETMC_PASSWORD, TSETMC_SCHEMA
import logging

# تنظیم لاگ‌گیری
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

headers = {
    "Content-Type": "text/xml; charset=utf-8",
    "SOAPAction": '"http://tsetmc.com/ClientType"'
}

ns = {
    'soap': 'http://schemas.xmlsoap.org/soap/envelope/',
    'ns': 'http://tsetmc.com/',
    'xs': 'http://www.w3.org/2001/XMLSchema'
}

logger.info("🔄 شروع دریافت داده‌های ClientType...")
soap_body = f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
               xmlns:xsd="http://www.w3.org/2001/XMLSchema"
               xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <ClientType xmlns="http://tsetmc.com/">
      <UserName>{TSETMC_USERNAME}</UserName>
      <Password>{TSETMC_PASSWORD}</Password>
    </ClientType>
  </soap:Body>
</soap:Envelope>"""

try:
    response = requests.post(
        "http://service.tsetmc.com/WebService/TsePublicV2.asmx",
        data=soap_body.encode('utf-8'),
        headers=headers
    )
    response.raise_for_status()
except requests.RequestException as e:
    logger.error(f"⛔️ خطا در درخواست HTTP: {e}")
    exit()

try:
    root = etree.fromstring(response.content)
except etree.XMLSyntaxError as e:
    logger.error(f"⛔️ خطا در پارس XML: {e}")
    exit()

client_type_result = root.find(
    './/soap:Body/ns:ClientTypeResponse/ns:ClientTypeResult', namespaces=ns
)

if client_type_result is None:
    logger.warning("⛔️ داده‌ای پیدا نشد")
    exit()

rows = []
expected_columns = [
    'InsCode', 'Buy_CountI', 'Buy_CountN', 'Buy_I_Volume', 'Buy_N_Volume',
    'Sell_CountI', 'Sell_CountN', 'Sell_I_Volume', 'Sell_N_Volume'
]

for data_elem in client_type_result.findall('.//Data'):
    row = {
        etree.QName(elem).localname: elem.text
        for elem in data_elem if etree.QName(elem).localname in expected_columns
    }
    rows.append(row)

if not rows:
    logger.warning("⚠️ هیچ ردیفی دریافت نشد")
    exit()

df = pd.DataFrame(rows)
if df.empty:
    logger.warning("⚠️ دیتافریم خالی است")
    exit()

# تبدیل نوع داده‌ها
df['InsCode'] = df['InsCode'].astype(str)
for col in ['Buy_CountI', 'Buy_CountN', 'Sell_CountI', 'Sell_CountN']:
    if col in df:
        df[col] = df[col].astype('Int64')
for col in ['Buy_I_Volume', 'Buy_N_Volume', 'Sell_I_Volume', 'Sell_N_Volume']:
    if col in df:
        df[col] = df[col].astype(float)

# گرفتن کدهای موجود برای جلوگیری از تکرار
with engine.connect() as conn:
    existing_codes = pd.read_sql(
        text(f"SELECT InsCode FROM {TSETMC_SCHEMA}.client_type"),
        conn
    )['InsCode'].tolist()

# فیلتر رکوردهای جدید
new_df = df[~df['InsCode'].isin(existing_codes)]

if not new_df.empty:
    new_df.to_sql('client_type', engine, schema=TSETMC_SCHEMA, if_exists='append', index=False)
    logger.info(f"✅ {len(new_df)} رکورد جدید اضافه شد")
else:
    logger.info("ℹ️ هیچ رکورد جدیدی برای درج وجود ندارد")
