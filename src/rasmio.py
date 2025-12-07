from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time

# ================== تنظیمات ==================
PHONE_NUMBER = "09382110315"

# XPathهای دقیق شما
LOGIN_BUTTON_XPATH      = "/html/body/div[8]/div/div[1]/div[1]/div[2]/button[2]"
PHONE_FIELD_XPATH       = "/html/body/div[1]/div/div[1]/div[2]/form/div[1]/div/input"
SEND_CODE_BUTTON_XPATH  = "/html/body/div[1]/div/div[1]/div[2]/form/button[1]"
CODE_FIELD_XPATH        = "/html/body/div[1]/div/div[1]/div[2]/div[2]/input"

options = webdriver.ChromeOptions()
options.add_argument("--start-maximized")
# options.add_argument("--disable-blink-features=AutomationControlled")  # اختیاری برای مخفی کردن ربات

driver = webdriver.Chrome(options=options)
wait = WebDriverWait(driver, 20)

try:
    driver.get("https://rasmio.com/")
    print("در حال ورود به رسمـیو...")

    # ۱. دکمه ورود
    wait.until(EC.element_to_be_clickable((By.XPATH, LOGIN_BUTTON_XPATH))).click()
    print("دکمه ورود کلیک شد ✓")

    # ۲. شماره موبایل
    phone_field = wait.until(EC.presence_of_element_located((By.XPATH, PHONE_FIELD_XPATH)))
    phone_field.clear()
    phone_field.send_keys(PHONE_NUMBER)
    print(f"شماره {PHONE_NUMBER} وارد شد ✓")

    # ۳. ارسال کد
    wait.until(EC.element_to_be_clickable((By.XPATH, SEND_CODE_BUTTON_XPATH))).click()
    print("کد ۵ رقمی ارسال شد...")

    # ۴. دریافت کد ۵ رقمی از کاربر
    while True:
        code = input("\nکد ۵ رقمی را وارد کنید: ").strip()
        if len(code) == 5 and code.isdigit():
            break
        print("کد باید دقیقاً ۵ رقم باشد!")

    # ۵. وارد کردن کد تأیید — اصلاح شده!
    print("در حال وارد کردن کد تأیید...")
    code_field = wait.until(EC.presence_of_element_located((By.XPATH, CODE_FIELD_XPATH)))
    code_field.clear()                    # اینجا درست است
    code_field.send_keys(code)            # اینجا درست است
    print(f"کد {code} با موفقیت وارد شد ✓")

    # ۶. معمولاً بعد از ۵ رقم خودش وارد می‌شه، ولی برای اطمینان ۲ ثانیه صبر می‌کنیم
    time.sleep(3)
    print("\nورود با موفقیت انجام شد! الان داخل حساب هستید.")

    input("\nبرای بستن مرورگر Enter بزنید...")

except Exception as e:
    print(f"خطا رخ داد: {e}")
    input("Enter بزنید تا مرورگر بسته شود...")

finally:
    driver.quit()