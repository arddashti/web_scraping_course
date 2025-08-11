import requests
import pandas as pd
from lxml import etree
from sqlalchemy import create_engine, text
import urllib

# اطلاعات ورود به وب‌سرویس TSETMC برای سرویس Auction
username = "novinib.com"
password = "n07!1\\1!13.Com04"
url = "http://service.tsetmc.com/WebService/TsePublicV2.asmx"

headers = {
    "Content-Type": "text/xml; charset=utf-8",
    "SOAPAction": '"http://tsetmc.com/Auction"'
}

# اتصال به دیتابیس SQL Server
server = 'localhost'
database = 'test'
username_sql = 'sa'
password_sql = 'Ada@20215'

params = urllib.parse.quote_plus(
    f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username_sql};PWD={password_sql}'
)
engine = create_engine(f'mssql+pyodbc:///?odbc_connect={params}')

ns = {
    'soap': 'http://schemas.xmlsoap.org/soap/envelope/',
    'ns': 'http://tsetmc.com/'
}

# ایجاد schema tsetmc_api
with engine.begin() as conn:
    conn.execute(text("""
    IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'tsetmc_api')
        EXEC('CREATE SCHEMA tsetmc_api');
    """))

# ایجاد جدول auction
with engine.begin() as conn:
    conn.execute(text("""
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'tsetmc_api' AND TABLE_NAME = 'auction')
    BEGIN
        CREATE TABLE tsetmc_api.auction (
            Idn NVARCHAR(50) PRIMARY KEY,
            Message NVARCHAR(255) NULL,
            MessageType NVARCHAR(50) NULL,
            Deven NVARCHAR(50) NULL,
            Heven NVARCHAR(50) NULL,
            AuctionId NVARCHAR(50) NULL
        );
    END
    """))

# دریافت Idn های موجود برای جلوگیری از تکرار
with engine.connect() as conn:
    existing_ids_df = pd.read_sql("SELECT Idn FROM tsetmc_api.auction", conn)
    existing_ids = set(existing_ids_df["Idn"].dropna().astype(str).unique())

print(f"📦 تعداد {len(existing_ids)} رکورد Idn در جدول auction موجود است")

# مقدار پارامتر from را مشخص کنید (مثلا 0)
from_value = 0

# ساخت بدنه درخواست SOAP با پارامتر from
soap_body = f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
               xmlns:xsd="http://www.w3.org/2001/XMLSchema"
               xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <Auction xmlns="http://tsetmc.com/">
      <UserName>{username}</UserName>
      <Password>{password}</Password>
      <from>{from_value}</from>
    </Auction>
  </soap:Body>
</soap:Envelope>"""

response = requests.post(url, data=soap_body.encode('utf-8'), headers=headers)

if response.status_code != 200:
    print(f"⛔️ خطا در دریافت داده از سرویس Auction: وضعیت HTTP {response.status_code}")
    exit()

root = etree.fromstring(response.content)
auction_result = root.find('.//soap:Body/ns:AuctionResponse/ns:AuctionResult', namespaces=ns)

if auction_result is None:
    print("⛔️ داده‌ای از سرویس Auction دریافت نشد")
    exit()

rows = []
# فرض اینکه هر رکورد در زیرتگ AuctionResult به صورت <Auction> است
for auction_item in auction_result.findall('.//Auction'):
    row = {etree.QName(elem).localname: elem.text for elem in auction_item}
    rows.append(row)

if not rows:
    print("⚠️ هیچ ردیفی دریافت نشد")
    exit()

df = pd.DataFrame(rows)
df['Idn'] = df['Idn'].astype(str)

df_new = df[~df['Idn'].isin(existing_ids)]

if df_new.empty:
    print("ℹ️ همه رکوردهای Idn قبلاً درج شده‌اند")
else:
    df_new.to_sql('auction', schema='tsetmc_api', con=engine, if_exists='append', index=False)
    print(f"✅ تعداد {len(df_new)} ردیف جدید درج شد")
