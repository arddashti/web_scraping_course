import requests
import pandas as pd
from lxml import etree
from sqlalchemy import text
from config import engine, TSETMC_USERNAME, TSETMC_PASSWORD, TSETMC_URL, TSETMC_SCHEMA
import logging

# تنظیم لاگ‌گیری
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# هدرهای مورد نیاز برای SOAP Request
headers = {
    "Content-Type": "text/xml; charset=utf-8",
    "SOAPAction": '"http://tsetmc.com/BestLimitOneIns"'
}

# تعریف namespaceهای XML برای جست‌وجوی عناصر خاص
ns = {
    'soap': 'http://schemas.xmlsoap.org/soap/envelope/',
    'ns': 'http://tsetmc.com/',
    'xs': 'http://www.w3.org/2001/XMLSchema'
}

# دریافت InsCodeها از جدول instrument
with engine.connect() as conn:
    inscodes_df = pd.read_sql(
        f"SELECT InsCode FROM {TSETMC_SCHEMA}.instrument WHERE Valid = 1",
        conn
    )
    inscodes = inscodes_df['InsCode'].astype(str).tolist()

logger.info(f"📦 تعداد {len(inscodes)} InsCode معتبر از جدول instrument دریافت شد")

# دریافت کلیدهای موجود در جدول best_limits برای جلوگیری از درج تکراری
with engine.connect() as conn:
    existing_codes = pd.read_sql(
        f"SELECT InsCode, number FROM {TSETMC_SCHEMA}.best_limits",
        conn
    )
    existing_keys = set(
        existing_codes.apply(lambda x: f"{x['InsCode']}_{x['number']}", axis=1)
    )

logger.info(f"📦 تعداد {len(existing_keys)} کلید ترکیبی (InsCode, number) در جدول best_limits موجود است")

# اجرای درخواست SOAP برای هر InsCode
all_data = []
for ins_code in inscodes:
    logger.info(f"🔄 شروع پردازش InsCode {ins_code}...")

    soap_body = f"""<?xml version="1.0" encoding="utf-8"?>
    <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                   xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                   xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
      <soap:Body>
        <BestLimitOneIns xmlns="http://tsetmc.com/">
          <UserName>{TSETMC_USERNAME}</UserName>
          <Password>{TSETMC_PASSWORD}</Password>
          <InsCode>{ins_code}</InsCode>
        </BestLimitOneIns>
      </soap:Body>
    </soap:Envelope>"""

    try:
        # ارسال درخواست SOAP
        response = requests.post(TSETMC_URL, data=soap_body.encode('utf-8'), headers=headers)
        response.raise_for_status()  # بررسی وضعیت پاسخ HTTP
    except requests.RequestException as e:
        logger.error(f"⛔️ InsCode {ins_code}: خطا در درخواست HTTP: {e}")
        logger.debug(f"پاسخ سرور: {response.text}")
        continue

    try:
        # پارس پاسخ XML
        root = etree.fromstring(response.content)
    except etree.XMLSyntaxError as e:
        logger.error(f"⛔️ InsCode {ins_code}: خطا در پارس XML: {e}")
        logger.debug(f"پاسخ خام XML: {response.text}")
        continue

    # استخراج داده‌های BestLimitOneInsResult
    best_limit_result = root.find('.//soap:Body/ns:BestLimitOneInsResponse/ns:BestLimitOneInsResult', namespaces=ns)

    if best_limit_result is None:
        logger.warning(f"⛔️ InsCode {ins_code}: داده‌ای پیدا نشد")
        continue

    rows = []
    # استخراج عناصر InstBestLimit
    for inst_best_limit in best_limit_result.findall('.//InstBestLimit'):
        row = {etree.QName(elem).localname: elem.text for elem in inst_best_limit}
        rows.append(row)

    if not rows:
        logger.warning(f"⚠️ InsCode {ins_code}: هیچ ردیفی دریافت نشد")
        continue

    # تبدیل به DataFrame
    df = pd.DataFrame(rows)
    if not df.empty:
        # تبدیل نوع داده‌ها
        df['InsCode'] = df['InsCode'].astype(str)
        df['number'] = df['number'].astype(int) if 'number' in df else None
        df['RefID'] = df['RefID'].astype(int) if 'RefID' in df else None
        df['QTitMeDem'] = df['QTitMeDem'].astype(int) if 'QTitMeDem' in df else None
        df['ZOrdMeDem'] = df['ZOrdMeDem'].astype(int) if 'ZOrdMeDem' in df else None
        df['PMeDem'] = df['PMeDem'].astype(float) if 'PMeDem' in df else None
        df['PMeOf'] = df['PMeOf'].astype(float) if 'PMeOf' in df else None
        df['ZOrdMeOf'] = df['ZOrdMeOf'].astype(int) if 'ZOrdMeOf' in df else None
        df['QTitMeOf'] = df['QTitMeOf'].astype(int) if 'QTitMeOf' in df else None
        if 'HEven' in df:
            df = df.rename(columns={'HEven': 'Heven'})  # اصلاح نام‌گذاری احتمالی
        if 'Heven' in df:
            df['Heven'] = df['Heven'].astype(int) if df['Heven'].notnull().any() else None

        # ایجاد کلید ترکیبی برای بررسی تکراری
        df['key'] = df.apply(lambda x: f"{x['InsCode']}_{x['number']}", axis=1)
        df_new = df[~df['key'].isin(existing_keys)]

        if df_new.empty:
            logger.info(f"ℹ️ InsCode {ins_code}: همه داده‌ها قبلاً درج شده‌اند")
            continue

        # حذف ستون key و ستون‌های غیرضروری قبل از ذخیره‌سازی
        df_new = df_new.drop(columns=['key'])
        if 'Heven' in df_new and 'Heven' not in pd.read_sql(f"SELECT TOP 1 * FROM {TSETMC_SCHEMA}.best_limits", engine).columns:
            df_new = df_new.drop(columns=['Heven'])

        # ذخیره داده‌ها در جدول
        try:
            with engine.begin() as conn:
                df_new.to_sql('best_limits', schema=TSETMC_SCHEMA, con=conn, if_exists='append', index=False)
            logger.info(f"✅ InsCode {ins_code}: {len(df_new)} ردیف جدید درج شد")
            existing_keys.update(df_new.apply(lambda x: f"{x['InsCode']}_{x['number']}", axis=1))
        except Exception as e:
            logger.error(f"⛔️ InsCode {ins_code}: خطا در درج داده‌ها: {e}")
            logger.debug(f"ستون‌های DataFrame: {df_new.columns.tolist()}")
            continue

        all_data.append(df_new)
    else:
        logger.warning(f"⚠️ InsCode {ins_code}: هیچ داده‌ای دریافت نشد")

# ترکیب و نمایش داده‌های جدید
if all_data:
    final_df = pd.concat(all_data, ignore_index=True)
    logger.info(f"📊 تعداد کل ردیف‌های جدید درج‌شده: {len(final_df)}")
    print(final_df)
else:
    logger.warning("⚠️ هیچ داده جدیدی دریافت نشد")