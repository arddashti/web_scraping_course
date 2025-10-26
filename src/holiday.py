import requests
from bs4 import BeautifulSoup

# آدرس صفحه سالانه تقویم
url = "https://www.time.ir/event-year"

# هدرها برای شبیه‌سازی مرورگر (برای جلوگیری از بلاک شدن درخواست)
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
}

# درخواست GET به سایت
response = requests.get(url, headers=headers)
response.encoding = 'utf-8'  # مهم برای نمایش صحیح حروف فارسی

# پارس کردن HTML
soup = BeautifulSoup(response.text, "html.parser")

# سلکتور مربوط به روزها و تاریخ‌ها در جدول تقویم
# (time.ir از ساختار table یا divهای با کلاس مشخص استفاده می‌کند)
day_items = soup.select(".eventyear-items .item")  # هر آیتم یک روز تقویمی است

data = []
for item in day_items:
    # متن داخل آیتم مثل: "روز غير تعطيل\n1404/09/11"
    text = item.get_text(strip=True)
    parts = text.split("\n")
    if len(parts) == 2:
        status = parts[0].strip()
        date = parts[1].strip()
        data.append({
            "status": status,
            "date": date
        })

# نمایش داده‌ها
for d in data:
    print(f"{d['status']}  |  {d['date']}")

# در صورت نیاز می‌توانید داده‌ها را در فایل CSV ذخیره کنید
import pandas as pd
df = pd.DataFrame(data)
df.to_csv("timeir_days_1404.csv", index=False, encoding="utf-8-sig")
print("\n✅ داده‌ها در فایل timeir_days_1404.csv ذخیره شدند.")
