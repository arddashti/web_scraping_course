import requests
import pandas as pd
from lxml import etree
from config import engine, TSETMC_USERNAME, TSETMC_PASSWORD, TSETMC_URL ,TSETMC_SCHEMA
from sqlalchemy import text


with engine.connect() as conn:
    result = conn.execute(text(f"SELECT MAX(Idn) AS last_idn FROM {TSETMC_SCHEMA}.auction")).mappings()
    row = result.fetchone()
    if row and row['last_idn'] is not None:
        last_idn = row['last_idn']
    else:
        last_idn = 0


print(f"✅ آخرین Idn از جدول خوانده شد: {last_idn}")

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
print(response.text[:1000])  # نمایش بخش ابتدایی پاسخ برای اطمینان

root = etree.fromstring(response.content)

# انتخاب تمام رکوردهای Data واقعی
data_nodes = root.xpath('//diffgr:diffgram//Data', 
                        namespaces={'diffgr': 'urn:schemas-microsoft-com:xml-diffgram-v1'})

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

df = pd.DataFrame(rows)
print(df)

if not df.empty:
    last_idn = df['Idn'].max()
    print(f"Updated last_idn: {last_idn}")

    # فقط ستون‌هایی که در جدول وجود دارند
    columns_in_db = ['Idn', 'AuctionID', 'MessageType', 'Deven', 'Heven', 'Message']
    df_new = df[columns_in_db]

    # بررسی داده‌های موجود برای جلوگیری از درج تکراری
    with engine.connect() as conn:
        query = f"SELECT Idn FROM {TSETMC_SCHEMA}.auction"
        existing_ids = pd.read_sql(query, conn)
        existing_ids_set = set(existing_ids['Idn'].astype(int))


    df_new = df_new[~df_new['Idn'].isin(existing_ids_set)]

    if df_new.empty:
        print("ℹ️ همه رکوردها قبلاً درج شده‌اند")
    else:
        # درج داده‌های جدید در جدول
        df_new.to_sql(
            'auction',
            con=engine,
            schema='tsetmc_api',  # مطمئن شو اسکما دقیقاً همان TSETMC_SCHEMA است
            if_exists='append',
            index=False
        )
        print(f"✅ {len(df_new)} ردیف جدید درج شد")
