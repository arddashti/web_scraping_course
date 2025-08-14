import requests
import pandas as pd
from lxml import etree
from sqlalchemy import create_engine, text
import urllib
from config import engine, TSETMC_USERNAME, TSETMC_PASSWORD, TSETMC_URL


headers = {
    "Content-Type": "text/xml; charset=utf-8",
    "SOAPAction": '"http://tsetmc.com/TradeOneDay"'
}


# --- تابع تبدیل پاسخ XML وب‌سرویس به DataFrame ---
def parse_tradeoneday_xml(xml_content):
    ns = {
        'soap': 'http://schemas.xmlsoap.org/soap/envelope/',
        'ns': 'http://tsetmc.com/'
    }
    root = etree.fromstring(xml_content)
    result = root.find('.//soap:Body/ns:TradeOneDayResponse/ns:TradeOneDayResult', namespaces=ns)
    if result is None:
        return pd.DataFrame()

    fields = {
        'LVal18AFC': str, 'DEven': int, 'ZTotTran': float,
        'QTotTran5J': float, 'QTotCap': float, 'InsCode': int,
        'LVal30': str, 'PClosing': float, 'PDrCotVal': float,
        'PriceChange': float, 'PriceMin': float, 'PriceMax': float,
        'PriceFirst': float, 'PriceYesterday': float
    }

    rows = []
    for trade in result.findall('.//TradeSelectedDate'):
        row = {}
        for elem in trade:
            tag = etree.QName(elem).localname
            if tag in fields:
                text = elem.text
                try:
                    row[tag] = fields[tag](text) if text is not None else None
                except:
                    row[tag] = None
        rows.append(row)
    return pd.DataFrame(rows)

# --- اجرای برنامه ---

# اطمینان از وجود اسکیمای tsetmc_api و جدول TradeOneDay
ensure_schema_and_table(engine)

# انتخاب تاریخ نمونه برای دریافت داده‌ها (YYYYMMDD)
sel_date = 20230801
flows = range(1, 8)  # تعداد بخش‌های داده برای دریافت

# خواندن کلیدهای موجود برای جلوگیری از تکراری شدن داده‌ها
with engine.connect() as conn:
    existing_keys = pd.read_sql("SELECT InsCode, DEven FROM tsetmc_api.TradeOneDay", conn)
existing_keys_set = set(zip(existing_keys['InsCode'], existing_keys['DEven']))
print(f"📦 {len(existing_keys_set)} کلید موجود در جدول")

# دریافت داده‌ها و درج در جدول
for flow in flows:
    print(f"\n🔁 شروع Flow {flow} برای تاریخ {sel_date}...")
    soap_body = f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
               xmlns:xsd="http://www.w3.org/2001/XMLSchema"
               xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <TradeOneDay xmlns="http://tsetmc.com/">
      <UserName>{TSETMC_USERNAME}</UserName>
      <Password>{TSETMC_PASSWORD}</Password>
      <SelDate>{sel_date}</SelDate>
      <Flow>{flow}</Flow>
    </TradeOneDay>
  </soap:Body>
</soap:Envelope>"""

    response = requests.post(TSETMC_URL, data=soap_body.encode('utf-8'), headers=headers)
    print("HTTP Status:", response.status_code)
    if response.status_code != 200:
        print("⚠️ درخواست موفق نبود، ادامه به Flow بعدی")
        continue

    df = parse_tradeoneday_xml(response.content)
    if df.empty:
        print("⚠️ داده‌ای دریافت نشد.")
        continue

    # حذف داده‌های تکراری بر اساس کلید (InsCode, DEven)
    df['key'] = list(zip(df['InsCode'], df['DEven']))
    df = df[~df['key'].isin(existing_keys_set)].drop(columns=['key'])

    if df.empty:
        print("ℹ️ همه داده‌ها قبلاً ذخیره شده‌اند")
        continue

    # اطمینان از ترتیب و وجود ستون‌ها قبل از درج
    expected_columns = [
        'LVal18AFC', 'DEven', 'ZTotTran', 'QTotTran5J', 'QTotCap',
        'InsCode', 'LVal30', 'PClosing', 'PDrCotVal', 'PriceChange',
        'PriceMin', 'PriceMax', 'PriceFirst', 'PriceYesterday'
    ]
    df = df[[col for col in expected_columns if col in df.columns]]

    # درج داده‌ها در جدول SQL Server
    df.to_sql('TradeOneDay', schema='tsetmc_api', con=engine, if_exists='append', index=False)
    print(f"✅ Flow {flow}: {len(df)} ردیف جدید درج شد")

    # اضافه کردن کلیدهای جدید به مجموعه کلیدها
    existing_keys_set.update(zip(df['InsCode'], df['DEven']))
