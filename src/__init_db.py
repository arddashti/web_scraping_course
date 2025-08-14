# init_db.py
from sqlalchemy import text
from config import engine, TSETMC_SCHEMA

with engine.begin() as conn:
    # ایجاد اسکما در صورت عدم وجود
    conn.execute(text(f"""
        IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{TSETMC_SCHEMA}')
        BEGIN
            EXEC('CREATE SCHEMA {TSETMC_SCHEMA}')
        END
    """))
    print(f"✅ بررسی و ایجاد اسکما {TSETMC_SCHEMA} انجام شد (در صورت عدم وجود)")

    # ایجاد جدول instrument
    conn.execute(text(f"""
        IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES 
                       WHERE TABLE_SCHEMA = '{TSETMC_SCHEMA}' AND TABLE_NAME = 'instrument')
        BEGIN
            EXEC('
                CREATE TABLE {TSETMC_SCHEMA}.instrument (
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
            ')
        END
    """))
    print(f"✅ جدول instrument بررسی و در صورت نیاز ایجاد شد")

    # ایجاد جدول adj_price_all
    conn.execute(text(f"""
        IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES 
                       WHERE TABLE_SCHEMA = '{TSETMC_SCHEMA}' AND TABLE_NAME = 'adj_price_all')
        BEGIN
            EXEC('
                CREATE TABLE {TSETMC_SCHEMA}.adj_price_all (
                    InsCode NVARCHAR(50) NOT NULL,
                    DEven INT NULL,
                    PClosing DECIMAL(18,4) NULL,
                    PClosingNoAdj DECIMAL(18,4) NULL,
                    Flow INT NULL,
                    CIsin NVARCHAR(50) NOT NULL
                );
            ')
        END
    """))
    print(f"✅ جدول adj_price_all بررسی و در صورت نیاز ایجاد شد")

    # ایجاد جدول adj_price
    conn.execute(text(f"""
        IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES 
                       WHERE TABLE_SCHEMA = '{TSETMC_SCHEMA}' AND TABLE_NAME = 'adj_price')
        BEGIN
            EXEC('
                CREATE TABLE {TSETMC_SCHEMA}.adj_price (
                    InsCode NVARCHAR(50) NOT NULL,
                    TseAdjPrice NVARCHAR(50) NULL,
                    DEven INT NULL,
                    PClosing DECIMAL(18,4) NULL,
                    PClosingNoAdj DECIMAL(18,4) NULL,
                    Flow INT NULL
                );
            ')
        END
    """))
    print(f"✅ جدول adj_price بررسی و در صورت نیاز ایجاد شد")

    # ایجاد جدول board
    conn.execute(text(f"""
        IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES 
                       WHERE TABLE_SCHEMA = '{TSETMC_SCHEMA}' AND TABLE_NAME = 'board')
        BEGIN
            EXEC('
                CREATE TABLE {TSETMC_SCHEMA}.board (
                    CComVal NVARCHAR(50) PRIMARY KEY,
                    LBoard NVARCHAR(200) NULL
                );
            ')
        END
    """))
    print(f"✅ جدول board بررسی و در صورت نیاز ایجاد شد")

    # ایجاد جدول TradeOneDay
    conn.execute(text(f"""
        IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES 
                       WHERE TABLE_SCHEMA = '{TSETMC_SCHEMA}' AND TABLE_NAME = 'TradeOneDay')
        BEGIN
            EXEC('
                CREATE TABLE {TSETMC_SCHEMA}.TradeOneDay (
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
                );
            ')
        END
    """))
    print(f"✅ جدول TradeOneDay بررسی و در صورت نیاز ایجاد شد")





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
