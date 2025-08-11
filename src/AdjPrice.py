import requests
import pandas as pd
from lxml import etree
from sqlalchemy import create_engine, text
import urllib

# اطلاعات ورود وب‌سرویس و دیتابیس
username = "novinib.com"
password = "n07!1\\1!13.Com04"
url = "http://service.tsetmc.com/webservice/TsePublicV2.asmx"

headers = {
    "Content-Type": "text/xml; charset=utf-8",
    "SOAPAction": '"http://tsetmc.com/AdjPrice"'
}

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
    'ns': 'http://tsetmc.com/',
    'diffgr': 'urn:schemas-microsoft-com:xml-diffgram-v1'
}

with engine.begin() as conn:
    conn.execute(text("""
        IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'tsetmc_api')
            EXEC('CREATE SCHEMA tsetmc_api');
    """))
    
    conn.execute(text("""
        IF NOT EXISTS (
            SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'tsetmc_api' AND TABLE_NAME = 'adj_price'
        )
        EXEC('
            CREATE TABLE tsetmc_api.adj_price (
                InsCode NVARCHAR(50) NOT NULL,
                TseAdjPrice NVARCHAR(50) NULL,
                DEven INT ,
                PClosing DECIMAL(18,4) NULL,
                PClosingNoAdj DECIMAL(18,4) NULL,
                Flow INT NULL
            );
        ')
    """))


# دریافت رکوردهای موجود برای جلوگیری از درج تکراری
with engine.connect() as conn:
    existing = pd.read_sql("""
        SELECT CONCAT(InsCode, '-', DEven) AS key_rec FROM tsetmc_api.adj_price
    """, conn)
    existing_keys = set(existing['key_rec'].tolist())

# دریافت داده‌ها برای Flow های 1 تا 7
for flow in range(1, 8):
    print(f"Flow {flow} در حال دریافت داده...")

    soap_body = f"""<?xml version="1.0" encoding="utf-8"?>
    <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" 
                   xmlns:xsd="http://www.w3.org/2001/XMLSchema" 
                   xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
      <soap:Body>
        <AdjPrice xmlns="http://tsetmc.com/">
          <UserName>{username}</UserName>
          <Password>{password}</Password>
          <Flow>{flow}</Flow>
        </AdjPrice>
      </soap:Body>
    </soap:Envelope>"""

    response = requests.post(url, data=soap_body.encode('utf-8'), headers=headers)

    root = etree.fromstring(response.content)
    diffgram = root.find('.//diffgr:diffgram', namespaces=ns)

    if diffgram is None or len(diffgram) == 0:
        print(f"هیچ داده‌ای برای Flow {flow} دریافت نشد")
        continue

    rows = []
    for adjprice in diffgram.findall('.//TseAdjPrice'):
        row = {etree.QName(child).localname: child.text for child in adjprice}
        row['Flow'] = flow
        rows.append(row)

    if not rows:
        print(f"هیچ داده‌ای در Flow {flow} موجود نیست")
        continue

    df = pd.DataFrame(rows)
    df['InsCode'] = df['InsCode'].astype(str)
    df['DEven'] = pd.to_numeric(df['DEven'], errors='coerce').astype('Int64')
    df['PClosing'] = pd.to_numeric(df['PClosing'], errors='coerce')
    df['PClosingNoAdj'] = pd.to_numeric(df['PClosingNoAdj'], errors='coerce')
    df['Flow'] = flow

    df['key_rec'] = df['InsCode'] + '-' + df['DEven'].astype(str)
    df_new = df[~df['key_rec'].isin(existing_keys)].drop(columns=['key_rec'])

    if df_new.empty:
        print(f"تمام داده‌ها در Flow {flow} قبلاً ذخیره شده‌اند")
        continue

    df_new.to_sql('adj_price', schema='tsetmc_api', con=engine, if_exists='append', index=False)
    print(f"{len(df_new)} رکورد جدید در Flow {flow} ذخیره شد")
    existing_keys.update(df_new['InsCode'] + '-' + df_new['DEven'].astype(str))

print("دریافت و ذخیره داده‌ها به پایان رسید.")
