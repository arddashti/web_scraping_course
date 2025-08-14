import requests
import pandas as pd
from lxml import etree
from sqlalchemy import create_engine, text
import urllib
from config import engine, TSETMC_USERNAME, TSETMC_PASSWORD, TSETMC_URL

# هدرهای مورد نیاز برای SOAP Request مخصوص Board
headers = {
    "Content-Type": "text/xml; charset=utf-8",
    "SOAPAction": '"http://tsetmc.com/Board"'
}

# تعریف namespaceهای XML برای جست‌وجوی عناصر خاص
ns = {
    'soap': 'http://schemas.xmlsoap.org/soap/envelope/',
    'ns': 'http://tsetmc.com/'
}


# دریافت CComValهای موجود برای جلوگیری از درج داده تکراری
with engine.connect() as conn:
    existing_codes = pd.read_sql("SELECT CComVal FROM tsetmc_api.board", conn)
    existing_ccomvals = set(existing_codes["CComVal"].dropna().astype(str).unique())

print(f"📦 تعداد {len(existing_ccomvals)} CComVal در جدول موجود است")

# ساخت بدنه SOAP برای درخواست Board
soap_body = f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
               xmlns:xsd="http://www.w3.org/2001/XMLSchema"
               xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <Board xmlns="http://tsetmc.com/">
      <UserName>{TSETMC_USERNAME}</UserName>
      <Password>{TSETMC_PASSWORD}</Password>
    </Board>
  </soap:Body>
</soap:Envelope>"""

# ارسال درخواست به وب‌سرویس
response = requests.post(TSETMC_URL, data=soap_body.encode('utf-8'), headers=headers)

# تجزیه XML پاسخ
root = etree.fromstring(response.content)

# استخراج داده‌ها
board_result = root.find('.//soap:Body/ns:BoardResponse/ns:BoardResult', namespaces=ns)
if board_result is None:
    print("⛔️ داده‌ای پیدا نشد")
    exit()

# استخراج ردیف‌های تابلوها
rows = []
for board in board_result.findall('.//TseBoardList'):
    row = {etree.QName(elem).localname: elem.text for elem in board}
    rows.append(row)

if not rows:
    print("⚠️ هیچ ردیفی دریافت نشد")
    exit()

df = pd.DataFrame(rows)

# اطمینان از رشته‌ای بودن CComVal برای مقایسه
df['CComVal'] = df['CComVal'].astype(str)

# فیلتر ردیف‌هایی که قبلا وجود دارند (جلوگیری از تکرار)
df_new = df[~df['CComVal'].isin(existing_ccomvals)]

if df_new.empty:
    print("ℹ️ همه CComValها قبلاً درج شده‌اند")
else:
    # درج داده‌های جدید در جدول
    df_new.to_sql('board', schema='tsetmc_api', con=engine, if_exists='append', index=False)
    print(f"✅ {len(df_new)} ردیف جدید درج شد")

"""
📘 راهنمای ستون‌های خروجی جدول tsetmc_api.board:

CComVal  : کد تابلو (کلید اصلی جدول)
LBoard   : نام تابلو
"""

