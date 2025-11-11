import time
import requests
import pandas as pd
from lxml import etree
from config import engine, TSETMC_USERNAME, TSETMC_PASSWORD, TSETMC_URL, TSETMC_SCHEMA
from sqlalchemy import text

def get_last_idn():
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT MAX(Idn) AS last_idn FROM {TSETMC_SCHEMA}.auction")).mappings()
        row = result.fetchone()
        return row['last_idn'] if row and row['last_idn'] is not None else 0

def fetch_auction_data(last_idn):
    headers = {
        "Content-Type": "text/xml; charset=utf-8",
        "SOAPAction": '"http://tsetmc.com/Auction"'
    }

    soap_body = f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
               xmlns:xsd="http://www.w3.org/2001/XMLSchema"
               xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <Auction xmlns="http://tsetmc.com/">
      <UserName>{TSETMC_USERNAME}</UserName>
      <Password>{TSETMC_PASSWORD}</Password>
      <from>{last_idn}</from>
    </Auction>
  </soap:Body>
</soap:Envelope>"""

    response = requests.post(TSETMC_URL, data=soap_body.encode('utf-8'), headers=headers)
    root = etree.fromstring(response.content)
    data_nodes = root.xpath('//diffgr:diffgram//Data', namespaces={'diffgr': 'urn:schemas-microsoft-com:xml-diffgram-v1'})
    
    rows = []
    for node in data_nodes:
        rows.append({
            'Idn': int(node.findtext('Idn') or 0),
            'Message': node.findtext('Message'),
            'MessageType': node.findtext('MessageType'),
            'Deven': node.findtext('Deven'),
            'Heven': node.findtext('Heven'),
            'AuctionID': node.findtext('AuctionID')
        })
    
    return pd.DataFrame(rows)

def insert_new_rows(df):
    if df.empty:
        print("ℹ️ داده‌ای برای درج وجود ندارد")
        return

    columns_in_db = ['Idn', 'AuctionID', 'MessageType', 'Deven', 'Heven', 'Message']
    df_new = df[columns_in_db]

    with engine.connect() as conn:
        existing_ids = pd.read_sql(f"SELECT Idn FROM {TSETMC_SCHEMA}.auction", conn)
        existing_ids_set = set(existing_ids['Idn'].astype(int))
    
    df_new = df_new[~df_new['Idn'].isin(existing_ids_set)]

    if df_new.empty:
        print("ℹ️ همه رکوردها قبلاً درج شده‌اند")
    else:
        df_new.to_sql('auction', con=engine, schema=TSETMC_SCHEMA, if_exists='append', index=False)
        print(f"✅ {len(df_new)} ردیف جدید درج شد")

# حلقه ۱۰۰ بار
for i in range(1000):
    print(f"\n🔄 اجرای بار {i+1}/5000")
    last_idn = get_last_idn()
    print(f"✅ آخرین Idn از جدول: {last_idn}")
    
    df = fetch_auction_data(last_idn)
    if not df.empty:
        last_idn = df['Idn'].max()
        print(f"Updated last_idn: {last_idn}")
    
    insert_new_rows(df)
    
    time.sleep(1)  # تأخیر ۱ ثانیه‌ای برای جلوگیری از فشار روی سرور
