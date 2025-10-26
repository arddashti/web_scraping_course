from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import time

# تنظیمات مرورگر
options = Options()
options.add_argument("--headless")  # برای اجرای بدون نمایش پنجره مرورگر
options.add_argument("--disable-blink-features=AutomationControlled")
options.add_argument("start-maximized")
options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:143.0) Gecko/20100101 Firefox/143.0")

# راه‌اندازی WebDriver
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

# آدرس صفحه اجاره آپارتمان تهران
url = "https://divar.ir/s/tehran/rent-apartment"
driver.get(url)
time.sleep(5)  # صبر برای بارگذاری اولیه صفحه

# اسکرول هوشمند برای بارگذاری بیشتر آگهی‌ها
for _ in range(5):  # تعداد دفعات اسکرول
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(3)

# استخراج اطلاعات آگهی‌ها
titles = []
prices = []
locations = []

posts = driver.find_elements(By.CSS_SELECTOR, "div.kt-post-card")
for post in posts:
    try:
        title = post.find_element(By.CSS_SELECTOR, "h2.kt-post-card__title").text
        price = post.find_element(By.CSS_SELECTOR, "div.kt-post-card__description").text
        location = post.find_element(By.CSS_SELECTOR, "div.kt-post-card__bottom-description").text
        titles.append(title)
        prices.append(price)
        locations.append(location)
    except Exception as e:
        print(f"خطا در استخراج داده‌ها: {e}")
        continue

driver.quit()

# ذخیره داده‌ها در فایل CSV
df = pd.DataFrame({
    "عنوان": titles,
    "قیمت": prices,
    "موقعیت": locations
})

df.to_csv("divar_rent_apartments.csv", index=False, encoding="utf-8-sig")
print(f"تعداد آگهی‌ها: {len(df)}")
print("داده‌ها با موفقیت در فایل divar_rent_apartments.csv ذخیره شد.")
