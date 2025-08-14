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
    "SOAPAction": '"http://tsetmc.com/BestLimitsAllIns"'
}

# تعریف namespaceهای XML برای جست‌وجوی عناصر خاص
ns = {
    'soap': 'http://schemas.xmlsoap.org/soap/envelope/',
    'ns': 'http://tsetmc.com/',
    'xs': 'http://www.w3.org/2001/XMLSchema'
}

# دریافت InsCodeها و numberهای موجود برای جلوگیری از درج تکراری
with engine.connect() as conn:
    existing_codes = pd.read_sql(
        f"SELECT InsCode, number, Flow FROM {TSETMC_SCHEMA}.best_limits",
        conn
    )
    existing_keys = set(
        existing_codes.apply(lambda x: f"{x['InsCode']}_{x['number']}_{x['Flow']}", axis=1)
    )

logger.info(f"📦 تعداد {len(existing_keys)} کلید ترکیبی (InsCode, number, Flow) در جدول موجود است")

# اجرای درخواست SOAP برای Flowهای 0 تا 5
all_data = []
for flow in range(6):  # Flowهای 0 تا 5
    logger.info(f"🔄 شروع Flow {flow}...")

    soap_body = f"""<?xml version="1.0" encoding="utf-8"?>
    <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                   xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                   xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
      <soap:Body>
        <BestLimitsAllIns xmlns="http://tsetmc.com/">
          <UserName>{TSETMC_USERNAME}</UserName>
          <Password>{TSETMC_PASSWORD}</Password>
          <Flow>{flow}</Flow>
        </BestLimitsAllIns>
      </soap:Body>
    </soap:Envelope>"""

    try:
        # ارسال درخواست SOAP
        response = requests.post(TSETMC_URL, data=soap_body.encode('utf-8'), headers=headers)
        response.raise_for_status()  # بررسی وضعیت پاسخ HTTP
    except requests.RequestException as e:
        logger.error(f"⛔️ Flow {flow}: خطا در درخواست HTTP: {e}")
        logger.debug(f"پاسخ سرور: {response.text}")
        continue

    try:
        # پارس پاسخ XML
        root = etree.fromstring(response.content)
    except etree.XMLSyntaxError as e:
        logger.error(f"⛔️ Flow {flow}: خطا در پارس XML: {e}")
        logger.debug(f"پاسخ خام XML: {response.text}")
        continue

    # استخراج داده‌های BestLimitsAllInsResult
    best_limits_result = root.find('.//soap:Body/ns:BestLimitsAllInsResponse/ns:BestLimitsAllInsResult', namespaces=ns)

    if best_limits_result is None:
        logger.warning(f"⛔️ Flow {flow}: داده‌ای پیدا نشد")
        continue

    rows = []
    # استخراج عناصر InstBestLimit
    for inst_best_limit in best_limits_result.findall('.//InstBestLimit'):
        row = {etree.QName(elem).localname: elem.text for elem in inst_best_limit}
        row["Flow"] = flow
        rows.append(row)

    if not rows:
        logger.warning(f"⚠️ Flow {flow}: هیچ ردیفی دریافت نشد")
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
        # ستون Heven حذف شده است، مگر اینکه تأیید شود که در جدول وجود دارد
        if 'HEven' in df:
            df = df.rename(columns={'HEven': 'Heven'})  # اصلاح نام‌گذاری احتمالی
        if 'Heven' in df:
            df['Heven'] = df['Heven'].astype(int) if df['Heven'].notnull().any() else None
        df['Flow'] = df['Flow'].astype(int)

        # ایجاد کلید ترکیبی برای بررسی تکراری
        df['key'] = df.apply(lambda x: f"{x['InsCode']}_{x['number']}_{x['Flow']}", axis=1)
        df_new = df[~df['key'].isin(existing_keys)]

        if df_new.empty:
            logger.info(f"ℹ️ Flow {flow}: همه داده‌ها قبلاً درج شده‌اند")
            continue

        # حذف ستون key و ستون‌های غیرضروری قبل از ذخیره‌سازی
        df_new = df_new.drop(columns=['key'])
        if 'Heven' in df_new and 'Heven' not in pd.read_sql(f"SELECT TOP 1 * FROM {TSETMC_SCHEMA}.best_limits", engine).columns:
            df_new = df_new.drop(columns=['Heven'])

        # ذخیره داده‌ها در جدول
        try:
            with engine.begin() as conn:
                df_new.to_sql('best_limits', schema=TSETMC_SCHEMA, con=conn, if_exists='append', index=False)
            logger.info(f"✅ Flow {flow}: {len(df_new)} ردیف جدید درج شد")
            existing_keys.update(df_new.apply(lambda x: f"{x['InsCode']}_{x['number']}_{x['Flow']}", axis=1))
        except Exception as e:
            logger.error(f"⛔️ Flow {flow}: خطا در درج داده‌ها: {e}")
            logger.debug(f"ستون‌های DataFrame: {df_new.columns.tolist()}")
            continue

        all_data.append(df_new)
    else:
        logger.warning(f"⚠️ Flow {flow}: هیچ داده‌ای دریافت نشد")

# ترکیب و نمایش داده‌های جدید
if all_data:
    final_df = pd.concat(all_data, ignore_index=True)
    logger.info(f"📊 تعداد کل ردیف‌های جدید درج‌شده: {len(final_df)}")
    print(final_df)
else:
    logger.warning("⚠️ هیچ داده جدیدی دریافت نشد")
