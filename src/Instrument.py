import requests  # برای ارسال درخواست HTTP به وب‌سرویس
import pandas as pd  # برای کار با داده‌های جدولی
from lxml import etree  # برای پارس کردن XML
from sqlalchemy import create_engine, text  # برای اتصال به دیتابیس با SQLAlchemy
import urllib  # برای رمزگذاری رشته‌ی اتصال به SQL Server

# اطلاعات ورود به وب‌سرویس TSETMC
username = "novinib.com"
password = "n07!1\\1!13.Com04"
# آدرس وب‌سرویس SOAP
url = "http://service.tsetmc.com/webservice/TsePublicV2.asmx"

# هدرهای مورد نیاز برای SOAP Request
headers = {
    "Content-Type": "text/xml; charset=utf-8",
    "SOAPAction": '"http://tsetmc.com/Instrument"'
}

# اطلاعات اتصال به دیتابیس SQL Server
#server = '10.120.148.101'
server = 'localhost'
database = 'test'
username_sql = 'sa'
password_sql = 'Ada@20215'

# ساخت رشته اتصال ODBC برای SQLAlchemy
params = urllib.parse.quote_plus(
    f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username_sql};PWD={password_sql}'
)
engine = create_engine(f'mssql+pyodbc:///?odbc_connect={params}')  # ساخت Engine اتصال

# تعریف namespaceهای XML برای جست‌وجوی عناصر خاص
ns = {
    'soap': 'http://schemas.xmlsoap.org/soap/envelope/',
    'ns': 'http://tsetmc.com/'
}

# ایجاد schema tsetmc_api در صورت عدم وجود با commit خودکار
with engine.begin() as conn:
    create_schema_sql = """
    IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'tsetmc_api')
    BEGIN
        EXEC('CREATE SCHEMA tsetmc_api');
    END
    """
    conn.execute(text(create_schema_sql))
    print("✅ بررسی و ایجاد schema tsetmc_api (در صورت عدم وجود) انجام شد")

# ایجاد جدول instrument در schema tsetmc_api در صورت عدم وجود با commit خودکار
with engine.begin() as conn:
    create_table_sql = """
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'tsetmc_api' AND TABLE_NAME = 'instrument')
    BEGIN
        CREATE TABLE tsetmc_api.instrument (
            InsCode NVARCHAR(50) PRIMARY KEY,
            DEVen NVARCHAR(50) NULL,
            InstrumentID NVARCHAR(50) NULL,
            CValMne NVARCHAR(50) NULL,
            LVal18 NVARCHAR(100) NULL,
            CSocCSAC NVARCHAR(50) NULL,
            LSoc30 NVARCHAR(100) NULL,
            LVal18AFC NVARCHAR(100) NULL,
            LVal30 NVARCHAR(100) NULL,
            CIsin NVARCHAR(50) NULL,
            QNmVlo NVARCHAR(50) NULL,
            ZTitad NVARCHAR(50) NULL,
            DESop NVARCHAR(50) NULL,
            YOPSJ NVARCHAR(50) NULL,
            CGdSVal NVARCHAR(50) NULL,
            CGrValCot NVARCHAR(50) NULL,
            DInMar NVARCHAR(50) NULL,
            YUniExpP NVARCHAR(50) NULL,
            YMarNSC NVARCHAR(50) NULL,
            CComVal NVARCHAR(50) NULL,
            CSecVal NVARCHAR(50) NULL,
            CSoSecVal NVARCHAR(50) NULL,
            YDeComp NVARCHAR(50) NULL,
            PSaiSMaxOkValMdv NVARCHAR(50) NULL,
            PSaiSMinOkValMdv NVARCHAR(50) NULL,
            BaseVol NVARCHAR(50) NULL,
            YVal NVARCHAR(50) NULL,
            QPasCotFxeVal NVARCHAR(50) NULL,
            QQtTranMarVal NVARCHAR(50) NULL,
            Flow INT NULL,
            QtitMinSaiOmProd NVARCHAR(50) NULL,
            QtitMaxSaiOmProd NVARCHAR(50) NULL,
            Valid INT NULL
        );
    END
    """
    conn.execute(text(create_table_sql))
    print("✅ بررسی و ایجاد جدول tsetmc_api.instrument (در صورت عدم وجود) انجام شد")

# دریافت InsCodeهای موجود برای جلوگیری از درج تکراری
with engine.connect() as conn:
    existing_codes = pd.read_sql("SELECT InsCode FROM tsetmc_api.instrument", conn)
    existing_inscodes = set(existing_codes["InsCode"].dropna().astype(str).unique())

print(f"📦 تعداد {len(existing_inscodes)} InsCode در جدول موجود است")

# اجرای درخواست SOAP برای Flow های 1 تا 7
for flow in range(1, 8):
    print(f"\n🔄 شروع Flow {flow}...")

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

    root = etree.fromstring(response.content)

    instrument_result = root.find('.//soap:Body/ns:InstrumentResponse/ns:InstrumentResult', namespaces=ns)

    if instrument_result is None:
        print(f"⛔️ Flow {flow}: داده‌ای پیدا نشد")
        continue

    rows = []
    for tse_instrument in instrument_result.findall('.//TseInstruments'):
        row = {etree.QName(elem).localname: elem.text for elem in tse_instrument}
        row["Flow"] = flow
        rows.append(row)

    if not rows:
        print(f"⚠️ Flow {flow}: هیچ ردیفی دریافت نشد")
        continue

    df = pd.DataFrame(rows)
    df['InsCode'] = df['InsCode'].astype(str)

    df_new = df[~df['InsCode'].isin(existing_inscodes)]

    if df_new.empty:
        print(f"ℹ️ Flow {flow}: همه InsCodeها قبلاً درج شده‌اند")
        continue

    df_new.to_sql('instrument', schema='tsetmc_api', con=engine, if_exists='append', index=False)

    print(f"✅ Flow {flow}: {len(df_new)} ردیف جدید درج شد")

    existing_inscodes.update(df_new['InsCode'].tolist())

"""
📘 راهنمای ستون‌های خروجی DataFrame و جدول tsetmc_api.instrument:

DEven              : تاریخ - نمادهایی که در آخرین روز معاملاتی وجود نداشته باشند دارای تاریخ قدیمی هستند
InsCode            : کد داخلی یکتا برای شناسایی نماد - کلید اصلی اطلاعات
InstrumentID       : کد 12 رقمی لاتین نماد
CValMne            : کد 5 رقمی لاتین نماد
LVal18             : نام 18 رقمی لاتین نماد
CSocCSAC           : کد 5 رقمی لاتین شرکت
LSoc30             : نام 30 رقمی فارسی شرکت
LVal18AFC          : کد 18 رقمی فارسی نماد
LVal30             : نام 30 رقمی فارسی نماد
CIsin              : کد 12 رقمی شرکت (ISIN)
QNmVlo             : قیمت اسمی
ZTitad             : تعداد سهام / سرمایه ثبت شده
DESop              : تاریخ تغییر نماد
YOPSJ              : نوع تغییر امروز نماد
CGdSVal            : نوع نماد (A - I - O و غیره)
CGrValCot          : کد گروه نماد
DInMar             : تاریخ اولین روز معاملاتی نماد
YUniExpP           : نوع واحد قیمت (1: قیمت، 2: درصد)
YMarNSC            : کد بازار (NO: بازار عادی، OL: odd-lot و ...)

CComVal            : کد تابلو
CSecVal            : کد گروه صنعت
CSoSecVal          : کد زیرگروه صنعت
YDeComp            : میزان تأخیر در تسویه

PSaiSMaxOkValMdv   : حداکثر قیمت مجاز
PSaiSMinOkValMdv   : حداقل قیمت مجاز
BaseVol            : حجم مبنا

YVal               : نوع نماد - مقادیر مختلف مانند:
                     67: شاخص قیمت، 200: اوراق مشارکت انرژی، 300: سهام عادی، 305: صندوق سرمایه‌گذاری و غیره

QPasCotFxeVal      : کوچک‌ترین واحد قیمت قابل معامله (Tick Size)
QQtTranMarVal      : کوچک‌ترین تعداد قابل معامله

Flow               : شناسه بازار (0: عمومی، 1: بورس، 2: فرابورس، 3: مشتقه، 4-5: پایه فرابورس، 6: انرژی، 7: کالا)

QtitMinSaiOmProd   : حداقل حجم سفارش مجاز
QtitMaxSaiOmProd   : حداکثر حجم سفارش مجاز

Valid              : وضعیت اعتبار نماد (0: حذف شده / قدیمی، 1: معتبر)
"""
