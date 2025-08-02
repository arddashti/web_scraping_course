import requests
import pandas as pd
from lxml import etree
from sqlalchemy import create_engine, text
import urllib

# --- تنظیمات ورود به وب‌سرویس ---
username = "novinib.com"
password = "n07!1\\1!13.Com04"
url = "http://service.tsetmc.com/WebService/TsePublicV2.asmx"

headers = {
    "Content-Type": "text/xml; charset=utf-8",
    "SOAPAction": '"http://tsetmc.com/TradeOneDay"'
}

# --- تنظیمات اتصال به دیتابیس SQL Server ---
server = '10.120.148.101'
database = 'test'
username_sql = 'sa'
password_sql = 'Ada@20215'

params = urllib.parse.quote_plus(
    f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username_sql};PWD={password_sql}'
)
engine = create_engine(f'mssql+pyodbc:///?odbc_connect={params}')

# --- تابع ایجاد اسکیمای tsetmc_api و جدول TradeOneDay در صورت عدم وجود ---
def ensure_schema_and_table(engine):
    with engine.begin() as conn:
        # ایجاد schema در صورت نبودن
        conn.execute(text("""
            IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'tsetmc_api')
            EXEC('CREATE SCHEMA tsetmc_api')
        """))

        # بررسی وجود جدول
        table_exists = conn.execute(text("""
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = 'tsetmc_api' AND TABLE_NAME = 'TradeOneDay'
        """)).scalar()

        # ایجاد جدول در صورت نبودن
        if table_exists == 0:
            conn.execute(text("""
                CREATE TABLE tsetmc_api.TradeOneDay (
                    LVal18AFC NVARCHAR(255),
                    DEven INT,
                    ZTotTran DECIMAL(38,10),
                    QTotTran5J DECIMAL(38,10),
                    QTotCap DECIMAL(38,10),
                    InsCode BIGINT,
                    LVal30 NVARCHAR(255),
                    PClosing DECIMAL(38,10),
                    PDrCotVal DECIMAL(38,10),
                    PriceChange DECIMAL(38,10),
                    PriceMin DECIMAL(38,10),
                    PriceMax DECIMAL(38,10),
                    PriceFirst DECIMAL(38,10),
                    PriceYesterday DECIMAL(38,10)
                    
                )
            """))
        print("✅ جدول TradeOneDay بررسی یا ایجاد شد.")

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
      <UserName>{username}</UserName>
      <Password>{password}</Password>
      <SelDate>{sel_date}</SelDate>
      <Flow>{flow}</Flow>
    </TradeOneDay>
  </soap:Body>
</soap:Envelope>"""

    response = requests.post(url, data=soap_body.encode('utf-8'), headers=headers)
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
