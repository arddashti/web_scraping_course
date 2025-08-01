import requests
import pandas as pd
from lxml import etree

username = "novinib.com"
password = "n07!1\\1!13.Com04"
flow = 2  # کد بازار (مثلاً 1 برای بورس)

url = "http://service.tsetmc.com/webservice/TsePublicV2.asmx"
headers = {
    "Content-Type": "text/xml; charset=utf-8",
    "SOAPAction": '"http://tsetmc.com/Instrument"'
}

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

if response.status_code != 200:
    print(f"❌ خطا: {response.status_code}")
    print(response.text)
    exit()

# پردازش پاسخ XML
tree = etree.fromstring(response.content)
body = tree.find('.//{http://schemas.xmlsoap.org/soap/envelope/}Body')
instrument_response = body[0]
instrument_result = instrument_response[0]

# استخراج داده‌های TseInstruments
ns = {'ns': 'http://tsetmc.com/'}
records = []
for tse_instrument in instrument_result.findall('.//ns:TseInstruments', namespaces=ns):
    record = {child.tag.split('}')[1]: child.text for child in tse_instrument}
    records.append(record)

df = pd.DataFrame(records)
print("✅ داده‌های Instrument دریافت شد:")
print(df.head())

df.to_csv("instrument_data.csv", index=False, encoding='utf-8-sig')
print("📁 ذخیره شد: instrument_data.csv")
