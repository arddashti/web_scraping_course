import requests  # برای ارسال درخواست HTTP به وب‌سرویس
import pandas as pd  # برای کار با داده‌های جدولی
from lxml import etree  # برای پارس کردن XML
from sqlalchemy import create_engine, text  # برای اتصال به دیتابیس با SQLAlchemy
import urllib  # برای رمزگذاری رشته‌ی اتصال به SQL Server
from config import engine, TSETMC_USERNAME, TSETMC_PASSWORD, TSETMC_URL

# هدرهای مورد نیاز برای SOAP Request
headers = {
    "Content-Type": "text/xml; charset=utf-8",
    "SOAPAction": '"http://tsetmc.com/Instrument"'
}

# تعریف namespaceهای XML برای جست‌وجوی عناصر خاص
ns = {
    'soap': 'http://schemas.xmlsoap.org/soap/envelope/',
    'ns': 'http://tsetmc.com/'
}

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
          <UserName>{TSETMC_USERNAME}</UserName>
          <Password>{TSETMC_PASSWORD}</Password>
          <Flow>{flow}</Flow>
        </Instrument>
      </soap:Body>
    </soap:Envelope>"""

    response = requests.post(TSETMC_URL, data=soap_body.encode('utf-8'), headers=headers)

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


