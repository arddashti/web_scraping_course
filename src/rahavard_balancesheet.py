import os
import urllib.parse
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import requests
import time
import pandas as pd
from datetime import datetime
from config import engine, TSETMC_USERNAME, TSETMC_PASSWORD, TSETMC_URL, TSETMC_SCHEMA

# بارگذاری متغیرهای محیطی
load_dotenv()

# ------------------------------
# 1. باز کردن مرورگر با Selenium برای دریافت کوکی‌ها
# ------------------------------
chrome_options = Options()
chrome_options.add_argument("--headless")  # مرورگر مخفی
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:141.0) Gecko/20100101 Firefox/141.0")

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
driver.get("https://rahavard365.com/asset/26054/balancesheet")
time.sleep(5)  # صبر برای لود کامل صفحه

# ------------------------------
# 2. گرفتن کوکی‌ها از Selenium
# ------------------------------
cookies = driver.get_cookies()
driver.quit()

# تبدیل کوکی‌ها به فرمت requests
cookies_dict = {cookie['name']: cookie['value'] for cookie in cookies}

# ------------------------------
# 3. درخواست مستقیم به API
# ------------------------------
url = "https://rahavard365.com/api/v2/fundamental/company-balance-sheets"
params = {
    "asset_id": 26054,
    "report_type_id": 1,
    "view_currency_id": 1,
    "view_type_id": 1,
    "combination_state_id": 2,
    "audit_state_id": 2,
    "representation_state_id": 2,
    "start_date": "2020-08-19T09:59:40.890Z",
    "end_date": "2025-08-19T10:59:40.890Z",
    "fiscal_year_order": "ASC",
    "font_type": "FA",
    "detail_state": "summary",
    "ratio_category_ids": "1,2,3,4",
    "sale_type_ids": 4,
    "detail_type_id": 1,
    "no_change_in_view_type": "true"
}
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:141.0) Gecko/20100101 Firefox/141.0",
    "Accept": "application/json, text/plain, */*",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "en-US,en;q=0.5",
    "Application-Name": "rahavard",
    "Connection": "keep-alive",
    "Content-Type": "application/json",
    "Referer": "https://rahavard365.com/asset/26054/balancesheet",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "TE": "trailers"
}

resp = requests.get(url, headers=headers, cookies=cookies_dict, params=params)
if resp.status_code != 200:
    print(f"دریافت داده موفق نبود! کد وضعیت: {resp.status_code}, متن پاسخ: {resp.text}")
    exit()

try:
    data = resp.json()
except ValueError:
    print("پاسخ دریافت‌شده JSON معتبر نیست!")
    exit()

# ------------------------------
# 4. پردازش داده‌ها و گروه‌بندی
# ------------------------------
# تعریف دسته‌بندی‌ها بر اساس عنوان‌های ردیف‌ها
def get_category(title, is_parent):
    if is_parent:
        if "جمع دارایی‌های جاری" in title:
            return "دارایی‌های جاری"
        elif "جمع دارایی‌های غیرجاری" in title:
            return "دارایی‌های غیرجاری"
        elif "جمع بدهی‌های جاری" in title:
            return "بدهی‌های جاری"
        elif "جمع بدهی‌های غیر جاری" in title:
            return "بدهی‌های غیرجاری"
        elif "جمع حقوق صاحبان سهام" in title:
            return "حقوق صاحبان سهام"
        elif "جمع کل دارایی‌ها" in title or "جمع کل بدهی‌ها و حقوق صاحبان سهام" in title:
            return "جمع کل"
        elif "جمع حقوق صاحبان سهام مصوب" in title:
            return "حقوق صاحبان سهام مصوب"
    return title  # برای ردیف‌های غیرجمع، عنوان به‌عنوان دسته‌بندی استفاده می‌شود

# استخراج نگاشت سال‌ها به کلیدهای عددی
fiscal_years = []
year_to_field = {}
for col in data['data']['column_definitions'][1:]:
    year = col['header_name'].replace("/", "_")
    fiscal_years.append(year)
    # استخراج کلید عددی (field) از انتهای سلسله‌مراتب
    field = col['children'][0]['children'][0]['children'][0]['children'][0]['children'][0]['children'][0].get('field')
    if field:
        year_to_field[year] = field
    else:
        print(f"Warning: No field found for year {year}")

# آماده‌سازی داده‌ها برای ذخیره
rows = []
for row in data['data']['row_data']:
    row_data = {
        'category': get_category(row['row_title']['title'], row['row_properties'].get('is_parent', False)),
        'title': row['row_title']['title'],
        'unit': row['row_properties']['data_content_type']['unit_label'],
        'is_parent': row['row_properties'].get('is_parent', False),
        'created_at': datetime.now()
    }
    # افزودن مقادیر برای هر سال مالی با استفاده از نگاشت
    for year in fiscal_years:
        field_key = year_to_field.get(year)
        if field_key:
            row_data[year] = row.get(field_key)
        else:
            row_data[year] = None
    rows.append(row_data)

# تبدیل به DataFrame
df = pd.DataFrame(rows)

# Unpivot کردن جدول
id_vars = ['category', 'title', 'unit', 'is_parent', 'created_at']
value_vars = fiscal_years
df_unpivoted = pd.melt(df, id_vars=id_vars, value_vars=value_vars, var_name='date_sh', value_name='value')

# تبدیل مقادیر NaN به None برای سازگاری با پایگاه داده
df_unpivoted['value'] = df_unpivoted['value'].where(df_unpivoted['value'].notna(), None)

# ------------------------------
# 5. ایجاد جدول در پایگاه داده
# ------------------------------
create_table_query = f"""
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'test_unpivoted' AND schema_id = SCHEMA_ID('{TSETMC_SCHEMA}'))
BEGIN
    CREATE TABLE {TSETMC_SCHEMA}.test_unpivoted (
        id INT IDENTITY(1,1) PRIMARY KEY,
        category NVARCHAR(100),
        title NVARCHAR(200),
        unit NVARCHAR(50),
        date_sh NVARCHAR(10),
        value BIGINT,
        is_parent BIT,
        created_at DATETIME
    )
END
"""
try:
    with engine.connect() as connection:
        connection.execute(text(create_table_query))
        connection.commit()
    print("جدول test_unpivoted با موفقیت ایجاد شد یا از قبل وجود داشت.")
except Exception as e:
    print(f"خطا در ایجاد جدول: {str(e)}")
    exit()

# ------------------------------
# 6. ذخیره داده‌ها در جدول
# ------------------------------
try:
    df_unpivoted.to_sql('test_unpivoted', engine, schema=TSETMC_SCHEMA, if_exists='append', index=False)
    print("داده‌ها با موفقیت در جدول test_unpivoted ذخیره شدند!")
except Exception as e:
    print(f"خطا در ذخیره داده‌ها: {str(e)}")