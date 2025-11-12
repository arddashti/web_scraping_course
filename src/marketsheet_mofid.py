from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import requests
import json
import time
from datetime import datetime
import sys
import os

# اضافه کردن مسیر src به Python path برای import config
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))
import config  # Import کردن config برای دسترسی به engine و تنظیمات

# ═══════════════════════════════════════════════════════════
# قسمت 1: تنظیمات اولیه
# ═══════════════════════════════════════════════════════════

# مسیر درایور و اطلاعات ورود
DRIVER_PATH = r'D:\web_scraping\web_scraping_course\chromedriver-win64\chromedriver.exe'
USERNAME = '0062980920'
PASSWORD = 'Ard136011@@'

# تنظیمات دریافت داده
API_URL = "https://api-mts.orbis.easytrader.ir/ms/api/MarketSheet/all/IRTKGANJ0001"
FETCH_INTERVAL = 5  # ثانیه
# OUTPUT_FILE = "market_data.json"  # دیگر استفاده نمی‌شود، داده‌ها به دیتابیس ذخیره می‌شوند

# ═══════════════════════════════════════════════════════════
# قسمت 2: توابع کمکی
# ═══════════════════════════════════════════════════════════

def setup_driver():
    """راه‌اندازی مرورگر با تنظیمات ضد-ربات"""
    options = webdriver.ChromeOptions()
    options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    
    # فعال کردن Performance Logging برای گرفتن Network Traffic
    options.set_capability('goog:loggingPrefs', {'performance': 'ALL'})
    
    service = Service(DRIVER_PATH)
    driver = webdriver.Chrome(service=service, options=options)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return driver

def login_and_get_token(driver):
    """لاگین به سیستم و استخراج توکن Authorization"""
    try:
        print("═" * 60)
        print("شروع فرآیند لاگین...")
        print("═" * 60)
        
        driver.get('https://login.emofid.com/Login')
        time.sleep(10)
        
        print(f"✓ صفحه لود شد: {driver.title}")
        
        # چک iframe
        iframes = driver.find_elements(By.TAG_NAME, 'iframe')
        if iframes:
            print(f"✓ تعداد iframe: {len(iframes)} - Switch به اولین")
            driver.switch_to.frame(iframes[0])
            time.sleep(2)
        
        # صبر برای لود صفحه
        wait = WebDriverWait(driver, 30)
        wait.until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
        
        # وارد کردن نام کاربری
        username_field = wait.until(EC.element_to_be_clickable((By.ID, 'user-name')))
        username_field.clear()
        username_field.send_keys(USERNAME)
        print("✓ نام کاربری وارد شد")
        
        # وارد کردن رمز عبور
        password_field = wait.until(EC.element_to_be_clickable((By.ID, 'password')))
        password_field.clear()
        password_field.send_keys(PASSWORD)
        print("✓ رمز عبور وارد شد")
        
        # کلیک دکمه لاگین
        login_selectors = [
            (By.CSS_SELECTOR, 'input[type="submit"]'),
            (By.CSS_SELECTOR, 'button[type="submit"]'),
            (By.NAME, 'Login'),
            (By.XPATH, '//input[@value="ورود"] | //button[contains(text(), "ورود")]')
        ]
        
        login_button = None
        for by, value in login_selectors:
            try:
                login_button = driver.find_element(by, value)
                break
            except NoSuchElementException:
                continue
        
        if not login_button:
            raise NoSuchElementException("دکمه لاگین پیدا نشد!")
        
        login_button.click()
        print("✓ دکمه لاگین کلیک شد")
        
        # ماکزیمم کردن پنجره مرورگر
        driver.maximize_window()
        print("✓ مرورگر به حالت تمام‌صفحه (ماکزیمم) درآمد")

        # صبر برای redirect به صفحه بعدی
        time.sleep(8)
        
        # برگشت به default content (اگر در iframe بودیم)
        driver.switch_to.default_content()
        
        # کلیک دکمه "ورود به ایزی تریدر"
        print("⏳ در حال جستجوی دکمه 'ورود به ایزی تریدر'...")
        
        # چند selector مختلف برای پیدا کردن دکمه
        easytrader_selectors = [
            (By.CSS_SELECTOR, 'a[href="https://d.easytrader.ir/"]'),
            (By.XPATH, '//a[contains(text(), "ورود به ایزی تریدر")]'),
            (By.XPATH, '//a[@aria-label="login to easytrader"]'),
            (By.CSS_SELECTOR, 'a.bg-m-green-50')
        ]
        
        easytrader_button = None
        for by, value in easytrader_selectors:
            try:
                easytrader_button = WebDriverWait(driver, 15).until(
                    EC.element_to_be_clickable((by, value))
                )
                print(f"✓ دکمه ایزی تریدر پیدا شد با {by}")
                break
            except TimeoutException:
                continue
        
        if easytrader_button:
            # فعال‌سازی Chrome DevTools Protocol برای گرفتن Network Traffic
            print("🔧 فعال‌سازی Network Monitoring...")
            driver.execute_cdp_cmd('Network.enable', {})
            
            easytrader_button.click()
            print("✓ دکمه 'ورود به ایزی تریدر' کلیک شد")
            print("⏳ در حال استخراج توکن از Network Traffic...")
            
            # صبر و گرفتن توکن از Network requests
            token = None
            start_time = time.time()
            timeout = 15  # 15 ثانیه timeout
            
            while time.time() - start_time < timeout:
                try:
                    # دریافت همه requestهای شبکه
                    logs = driver.get_log('performance')
                    
                    for entry in logs:
                        try:
                            log = json.loads(entry['message'])['message']
                            
                            # فقط requestهای Network.requestWillBeSent
                            if log['method'] == 'Network.requestWillBeSent':
                                request = log['params']['request']
                                url = request.get('url', '')
                                
                                # چک کردن URL مورد نظر
                                if 'api-mts.orbis.easytrader.ir' in url or 'easytrader.ir' in url:
                                    headers = request.get('headers', {})
                                    
                                    # جستجوی Authorization header
                                    for header_name, header_value in headers.items():
                                        if header_name.lower() == 'authorization':
                                            # استخراج توکن
                                            if header_value.startswith('Bearer '):
                                                token = header_value[7:].strip()
                                            else:
                                                token = header_value.strip()
                                            
                                            print(f"✓ توکن از Network Request استخراج شد!")
                                            print(f"  URL: {url[:80]}...")
                                            break
                                
                                if token:
                                    break
                        except:
                            continue
                    
                    if token:
                        break
                    
                    time.sleep(0.5)
                    
                except Exception as e:
                    time.sleep(0.5)
                    continue
            
            if not token:
                print("⚠ توکن از Network Traffic پیدا نشد. جستجو در localStorage...")
                time.sleep(3)
        else:
            print("⚠ دکمه 'ورود به ایزی تریدر' پیدا نشد - ادامه می‌دهیم...")
            token = None
        
        # اگر توکن از Network پیدا نشد، از localStorage بگیر
        if not token:
            try:
                print("🔍 جستجوی توکن در localStorage...")
                
                # اطمینان از بودن در صفحه درست
                if 'easytrader.ir' not in driver.current_url:
                    driver.get("https://d.easytrader.ir/")
                    time.sleep(3)
                
                # روش 1: localStorage
                token = driver.execute_script("return localStorage.getItem('access_token');")
                if token:
                    print("✓ توکن از localStorage پیدا شد")
                
                # روش 2: sessionStorage
                if not token:
                    token = driver.execute_script("return sessionStorage.getItem('access_token');")
                    if token:
                        print("✓ توکن از sessionStorage پیدا شد")
                
                # روش 3: جستجوی همه keyها
                if not token:
                    all_storage = driver.execute_script("""
                        let items = {};
                        for (let i = 0; i < localStorage.length; i++) {
                            let key = localStorage.key(i);
                            items[key] = localStorage.getItem(key);
                        }
                        return items;
                    """)
                    for key, value in all_storage.items():
                        if 'token' in key.lower() or 'auth' in key.lower():
                            token = value
                            print(f"✓ توکن از key '{key}' پیدا شد")
                            break
                        
            except Exception as e:
                print(f"⚠ خطا در استخراج از localStorage: {e}")
        
        if token:
            print("✓ توکن با موفقیت استخراج شد")
            print(f"  توکن (50 کاراکتر اول): {token[:50]}...")
            return token
        else:
            print("⚠ توکن یافت نشد - از Developer Tools آن را کپی کنید")
            print("  F12 > Application > Local Storage > access_token")
            return None
            
    except Exception as e:
        print(f"✗ خطا در لاگین: {e}")
        driver.save_screenshot('login_error.png')
        return None

def fetch_market_data(token):
    """دریافت داده‌های بازار با استفاده از توکن"""
    headers = {
        "Authorization": f"Bearer {token}",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "fa",
        "Referer": "https://d.easytrader.ir/",
        "Origin": "https://d.easytrader.ir"
    }
    
    try:
        response = requests.get(API_URL, headers=headers, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            # اضافه کردن timestamp
            data['timestamp'] = datetime.now().isoformat()
            data['fetch_time'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            return data
        elif response.status_code == 401:
            print(f"\n⚠️ خطای 401: توکن منقضی شده یا نامعتبر است!")
            print("   لطفاً توکن جدید از Developer Tools کپی کنید.")
            return None
        else:
            print(f"✗ خطا در درخواست API: {response.status_code}")
            print(f"   پاسخ: {response.text[:200]}")
            return None
    except Exception as e:
        print(f"✗ خطا در دریافت داده: {e}")
        return None

def save_to_database(data, engine):
    """ذخیره داده در دیتابیس SQL Server"""
    try:
        # فرض بر این است که جدول market_data در schema tsetmc_api وجود دارد
        # با فیلدهای: timestamp (datetime), fetch_time (varchar), data_json (nvarchar(max))
        # اگر جدول وجود ندارد، باید آن را ایجاد کنید (کد ایجاد جدول در انتها اضافه شده)
        
        import pandas as pd
        
        # تبدیل داده به DataFrame برای insert آسان
        df = pd.DataFrame([{
            'timestamp': data['timestamp'],
            'fetch_time': data['fetch_time'],
            'data_json': json.dumps(data, ensure_ascii=False)  # ذخیره کل داده به صورت JSON
        }])
        
        # Insert به جدول
        df.to_sql(
            name='market_data',
            con=engine,
            schema=config.TSETMC_SCHEMA,
            if_exists='append',
            index=False,
            method='multi'
        )
        
        print("✓ داده با موفقیت در دیتابیس ذخیره شد")
        return True
    except Exception as e:
        print(f"✗ خطا در ذخیره دیتابیس: {e}")
        return False

# ═══════════════════════════════════════════════════════════
# قسمت 3: برنامه اصلی
# ═══════════════════════════════════════════════════════════

def main():
    driver = None
    token = None
    
    try:
        # 1. راه‌اندازی مرورگر و لاگین
        driver = setup_driver()
        token = login_and_get_token(driver)
        
        if not token:
            print("\n" + "=" * 60)
            print("توکن یافت نشد! لطفاً آن را دستی کپی کنید:")
            print("1. در مرورگر F12 را بزنید")
            print("2. به تب Application بروید")
            print("3. Local Storage > https://d.easytrader.ir را باز کنید")
            print("4. مقدار 'access_token' را کپی کنید")
            print("   (فقط مقدار توکن، بدون 'Bearer')")
            print("=" * 60)
            token = input("\nتوکن را اینجا paste کنید: ").strip()
            # حذف "Bearer " اگر وجود داشته باشد
            if token.startswith("Bearer "):
                token = token[7:].strip()
                print("✓ 'Bearer' از ابتدای توکن حذف شد")
        
        if not token:
            print("✗ بدون توکن نمی‌توان ادامه داد!")
            return
        
        # پاکسازی توکن
        token = token.strip()
        if token.startswith("Bearer "):
            token = token[7:].strip()
        
        print(f"\n✓ توکن آماده است (طول: {len(token)} کاراکتر)")
        print(f"  اولین 30 کاراکتر: {token[:30]}...")
        print(f"  آخرین 30 کاراکتر: ...{token[-30:]}")
        
        print("\n" + "═" * 60)
        print("✓ آماده دریافت داده‌ها...")
        print(f"  فاصله زمانی: هر {FETCH_INTERVAL} ثانیه")
        print(f"  ذخیره در: دیتابیس {config.DB_NAME} (schema: {config.TSETMC_SCHEMA})")
        print("  برای توقف: Ctrl+C")
        print("═" * 60 + "\n")
        
        # 2. حلقه دریافت داده
        fetch_count = 0
        error_count = 0
        max_consecutive_errors = 10
        first_save_done = False
        
        while True:
            fetch_count += 1
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"[{timestamp}] دریافت #{fetch_count}...", end=" ")
            
            data = fetch_market_data(token)
            
            if data:
                error_count = 0  # ریست شمارنده خطا
                # ذخیره در دیتابیس
                if save_to_database(data, config.engine):
                    # نمایش خلاصه داده
                    buy_count = len(data.get('buySheets', []))
                    sell_count = len(data.get('sellSheets', []))
                    print(f"✓ (خرید: {buy_count}, فروش: {sell_count})")
                    
                    # بعد از اولین ذخیره موفق، درایور را ببند
                    if not first_save_done and driver:
                        print("✓ اولین ذخیره موفق انجام شد. بستن مرورگر...")
                        try:
                            driver.switch_to.default_content()
                            driver.quit()
                            driver = None
                            first_save_done = True
                            print("✓ مرورگر بسته شد.")
                        except Exception as e:
                            print(f"⚠ خطا در بستن مرورگر: {e}")
                            first_save_done = True
                else:
                    print("✗ خطا در ذخیره")
            else:
                error_count += 1
                print(f"✗ دریافت ناموفق (خطای متوالی: {error_count}/{max_consecutive_errors})")
                
                # اگر خطاهای متوالی زیاد شد، از کاربر توکن جدید بگیر
                if error_count >= max_consecutive_errors:
                    print("\n" + "!" * 60)
                    print(f"⚠️ {max_consecutive_errors} خطای متوالی! احتمالاً توکن منقضی شده است.")
                    print("!" * 60)
                    new_token = input("\nتوکن جدید را وارد کنید (یا Enter برای توقف): ").strip()
                    
                    if new_token:
                        if new_token.startswith("Bearer "):
                            new_token = new_token[7:].strip()
                        token = new_token
                        error_count = 0
                        print("✓ توکن جدید ثبت شد. ادامه می‌دهیم...\n")
                    else:
                        print("✗ توقف برنامه به دلیل عدم ارائه توکن جدید")
                        break
            
            # صبر تا دریافت بعدی
            time.sleep(FETCH_INTERVAL)
    
    except KeyboardInterrupt:
        print("\n\n" + "═" * 60)
        print("✓ برنامه توسط کاربر متوقف شد")
        print("✓ داده‌ها در دیتابیس ذخیره شدند")
        print("═" * 60)
    
    except Exception as e:
        print(f"\n✗ خطای غیرمنتظره: {e}")
    
    finally:
        if driver:
            print("\nبستن مرورگر...")
            try:
                driver.switch_to.default_content()
                driver.quit()
            except:
                pass

# کد اضافی: ایجاد جدول market_data اگر وجود نداشته باشد (یک بار اجرا کنید)
def create_table_if_not_exists(engine):
    """ایجاد جدول market_data اگر وجود نداشته باشد"""
    from sqlalchemy import text
    
    create_table_sql = """
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='market_data' AND xtype='U')
    BEGIN
        CREATE TABLE tsetmc_api.market_data (
            id INT IDENTITY(1,1) PRIMARY KEY,
            timestamp DATETIME2 NOT NULL,
            fetch_time NVARCHAR(50) NOT NULL,
            data_json NVARCHAR(MAX) NOT NULL,
            created_at DATETIME2 DEFAULT GETDATE()
        )
    END
    """
    
    with engine.connect() as conn:
        conn.execute(text(create_table_sql))
        conn.commit()
    print("✓ جدول market_data ایجاد شد یا از قبل وجود داشت.")

if __name__ == "__main__":
    # ایجاد جدول قبل از شروع (اختیاری، یک بار)
    create_table_if_not_exists(config.engine)
    main()