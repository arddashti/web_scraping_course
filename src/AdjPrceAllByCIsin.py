import requests
import pandas as pd
from lxml import etree
from sqlalchemy import create_engine, text
import urllib
import time

# اطلاعات ورود وب‌سرویس و دیتابیس
username = "novinib.com"
password = "n07!1\\1!13.Com04"
url = "http://service.tsetmc.com/webservice/TsePublicV2.asmx"

headers = {
    "Content-Type": "text/xml; charset=utf-8",
    "SOAPAction": '"http://tsetmc.com/AdjPrceAllByCIsin"'
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

# ایجاد اسکیمای tsetmc_api و جدول adj_price_all در صورت عدم وجود
with engine.begin() as conn:
    conn.execute(text("""
        IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'tsetmc_api')
            EXEC('CREATE SCHEMA tsetmc_api');
    """))
    
    conn.execute(text("""
        IF NOT EXISTS (
            SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'tsetmc_api' AND TABLE_NAME = 'adj_price_all'
        )
        EXEC('
            CREATE TABLE tsetmc_api.adj_price_all (
                InsCode NVARCHAR(50) NOT NULL,
                DEven INT NULL,
                PClosing DECIMAL(18,4) NULL,
                PClosingNoAdj DECIMAL(18,4) NULL,
                Flow INT NULL,
                CIsin NVARCHAR(50) NOT NULL
            );
        ')
    """))

# خواندن همه InsCode و CIsin های معتبر از جدول instrument
with engine.connect() as conn:
    df_ins = pd.read_sql("""
        SELECT DISTINCT InsCode, CIsin FROM tsetmc_api.instrument
        WHERE CIsin IS NOT NULL AND CIsin <> ''
    """, conn)

if df_ins.empty:
    print("هیچ نمادی با CIsin معتبر در جدول instrument یافت نشد.")
    exit()

# خواندن رکوردهای موجود برای جلوگیری از درج تکراری
with engine.connect() as conn:
    existing = pd.read_sql("""
        SELECT CONCAT(InsCode, '-', DEven, '-', CIsin) AS key_rec FROM tsetmc_api.adj_price_all
    """, conn)
    existing_keys = set(existing['key_rec'].tolist())

for idx, row in df_ins.iterrows():
    inscode = str(row['InsCode'])
    cisin = str(row['CIsin'])
    print(f"در حال دریافت داده برای InsCode={inscode}، CIsin={cisin}")

    soap_body = f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" 
               xmlns:xsd="http://www.w3.org/2001/XMLSchema" 
               xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <AdjPrceAllByCIsin xmlns="http://tsetmc.com/">
      <UserName>{username}</UserName>
      <Password>{password}</Password>
      <CIsin>{cisin}</CIsin>
    </AdjPrceAllByCIsin>
  </soap:Body>
</soap:Envelope>"""

    response = requests.post(url, data=soap_body.encode('utf-8'), headers=headers)
    
    root = etree.fromstring(response.content)
    diffgram = root.find('.//diffgr:diffgram', namespaces=ns)

    if diffgram is None or len(diffgram) == 0:
        print(f"هیچ داده‌ای برای CIsin {cisin} دریافت نشد")
        continue

    rows = []
    for data_node in diffgram.findall('.//Data/Data', namespaces={'diffgr': 'urn:schemas-microsoft-com:xml-diffgram-v1'}):
        row_data = {}
        for child in data_node:
            tag = etree.QName(child).localname
            row_data[tag] = child.text
        row_data['CIsin'] = cisin
        rows.append(row_data)

    if not rows:
        print(f"هیچ داده‌ای در CIsin {cisin} موجود نیست")
        continue

    df = pd.DataFrame(rows)

    # تبدیل ستون‌ها به نوع مناسب
    df['CValIsin'] = df['CValIsin'].astype(str)
    df['DevenHeven'] = pd.to_datetime(df['DevenHeven'], errors='coerce')
    df['PClosing'] = pd.to_numeric(df['PClosing'], errors='coerce')
    df['PClosingNoAdj'] = pd.to_numeric(df['PClosingNoAdj'], errors='coerce')
    df['CooperateType'] = df['CooperateType'].astype(str)

    # آماده‌سازی داده‌ها برای ذخیره در جدول adj_price_all
    df_to_save = pd.DataFrame({
        'InsCode': [inscode] * len(df),
        'DEven': df['DevenHeven'].dt.strftime('%Y%m%d').astype(int),  # تبدیل تاریخ به عدد yyyymmdd
        'PClosing': df['PClosing'],
        'PClosingNoAdj': df['PClosingNoAdj'],
        'Flow': pd.NA,
        'CIsin': df['CIsin']
    })

    # حذف رکوردهای تکراری
    df_to_save['key_rec'] = df_to_save['InsCode'] + '-' + df_to_save['DEven'].astype(str) + '-' + df_to_save['CIsin']
    df_new = df_to_save[~df_to_save['key_rec'].isin(existing_keys)].drop(columns=['key_rec'])

    if df_new.empty:
        print(f"تمام داده‌ها برای CIsin {cisin} قبلاً ذخیره شده‌اند")
    else:
        df_new.to_sql('adj_price_all', schema='tsetmc_api', con=engine, if_exists='append', index=False)
        print(f"{len(df_new)} رکورد جدید برای CIsin {cisin} ذخیره شد")
        existing_keys.update(df_new['InsCode'] + '-' + df_new['DEven'].astype(str) + '-' + df_new['CIsin'])

    # تاخیر کوتاه برای جلوگیری از فشار زیاد روی سرور وب‌سرویس
    time.sleep(1)

print("دریافت و ذخیره داده‌ها به پایان رسید.")
