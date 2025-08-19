from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import requests
import time

# ----------------------------
# 1. باز کردن مرورگر با Selenium برای دریافت کوکی‌ها
# ----------------------------
chrome_options = Options()
chrome_options.add_argument("--headless")  # مرورگر مخفی
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:141.0) Gecko/20100101 Firefox/141.0")

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
driver.get("https://rahavard365.com/asset/26054/balancesheet")
time.sleep(5)  # صبر برای لود کامل صفحه

# ----------------------------
# 2. گرفتن کوکی‌ها از Selenium
# ----------------------------
cookies = driver.get_cookies()
driver.quit()

# تبدیل کوکی‌ها به فرمت requests
cookies_dict = {cookie['name']: cookie['value'] for cookie in cookies}

# ----------------------------
# 3. درخواست مستقیم به API
# ----------------------------
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
print("HTTP Status Code:", resp.status_code)
if resp.status_code == 200:
    try:
        data = resp.json()
        print(data)
    except ValueError:
        print("پاسخ دریافت‌شده JSON معتبر نیست!")
else:
    print("دریافت داده موفق نبود! متن پاسخ:", resp.text)