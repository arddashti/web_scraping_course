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
import pytz

sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))
import config

# ═══════════════════════════════════════════════════════════
# قسمت 1: تنظیمات اولیه
# ═══════════════════════════════════════════════════════════
DRIVER_PATH = r'D:\web_scraping\web_scraping_course\chromedriver-win64\chromedriver.exe'
USERNAME = '0062980920'
PASSWORD = 'Ard136011@@'

API_URL_MARKET_SHEET = "https://api-mts.orbis.easytrader.ir/ms/api/MarketSheet/all/IRTKGANJ0001"
API_URL_TSETMC = "https://cdn.tsetmc.com/api/ClosingPrice/GetClosingPriceInfo/58514988269776425"
FETCH_INTERVAL = 5
MAX_TOKEN_REFRESH_ATTEMPTS = 3  # حداکثر تلاش برای تمدید توکن

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
    options.set_capability('goog:loggingPrefs', {'performance': 'ALL'})
    
    service = Service(DRIVER_PATH)
    driver = webdriver.Chrome(service=service, options=options)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return driver

def login_and_get_token(driver):
    """لاگین به سیستم و استخراج توکن Authorization"""
    try:
        print("\n" + "═" * 60)
        print("🔄 شروع فرآیند لاگین و دریافت توکن جدید...")
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
        
        driver.maximize_window()
        print("✓ مرورگر به حالت تمام‌صفحه (ماکزیمم) درآمد")
        time.sleep(8)
        
        driver.switch_to.default_content()
        
        print("⏳ در حال جستجوی دکمه 'ورود به ایزی تریدر'...")
        
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
            print("🔧 فعال‌سازی Network Monitoring...")
            driver.execute_cdp_cmd('Network.enable', {})
            
            easytrader_button.click()
            print("✓ دکمه 'ورود به ایزی تریدر' کلیک شد")
            print("⏳ در حال استخراج توکن از Network Traffic...")
            
            token = None
            start_time = time.time()
            timeout = 15
            
            while time.time() - start_time < timeout:
                try:
                    logs = driver.get_log('performance')
                    
                    for entry in logs:
                        try:
                            log = json.loads(entry['message'])['message']
                            
                            if log['method'] == 'Network.requestWillBeSent':
                                request = log['params']['request']
                                url = request.get('url', '')
                                
                                if 'api-mts.orbis.easytrader.ir' in url or 'easytrader.ir' in url:
                                    headers = request.get('headers', {})
                                    
                                    for header_name, header_value in headers.items():
                                        if header_name.lower() == 'authorization':
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
                
                if 'easytrader.ir' not in driver.current_url:
                    driver.get("https://d.easytrader.ir/")
                    time.sleep(3)
                
                token = driver.execute_script("return localStorage.getItem('access_token');")
                if token:
                    print("✓ توکن از localStorage پیدا شد")
                
                if not token:
                    token = driver.execute_script("return sessionStorage.getItem('access_token');")
                    if token:
                        print("✓ توکن از sessionStorage پیدا شد")
                
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
            # پاکسازی توکن
            token = token.strip()
            if token.startswith("Bearer "):
                token = token[7:].strip()
            
            print("✅ توکن با موفقیت استخراج شد")
            print(f"   طول توکن: {len(token)} کاراکتر")
            print(f"   اولین 30 کاراکتر: {token[:30]}...")
            print(f"   آخرین 30 کاراکتر: ...{token[-30:]}")
            return token
        else:
            print("❌ توکن یافت نشد")
            return None
            
    except Exception as e:
        print(f"✗ خطا در لاگین: {e}")
        driver.save_screenshot('login_error.png')
        return None

def refresh_token_if_needed(driver, token, token_refresh_count):
    """تمدید توکن در صورت انقضا"""
    print("\n" + "!" * 60)
    print(f"⚠️  توکن منقضی شده! تلاش #{token_refresh_count + 1} برای دریافت توکن جدید...")
    print("!" * 60)
    
    try:
        # اگر driver وجود ندارد، یکی جدید بساز
        if driver is None:
            print("🔧 ایجاد driver جدید...")
            driver = setup_driver()
        
        # دریافت توکن جدید
        new_token = login_and_get_token(driver)
        
        if new_token:
            print("\n" + "✅" * 30)
            print("✅ توکن جدید با موفقیت دریافت شد!")
            print("✅" * 30 + "\n")
            return new_token, driver, 0  # reset token_refresh_count
        else:
            print("❌ دریافت توکن جدید ناموفق بود")
            return None, driver, token_refresh_count + 1
            
    except Exception as e:
        print(f"❌ خطا در تمدید توکن: {e}")
        return None, driver, token_refresh_count + 1

def fetch_market_sheet_data(token):
    """دریافت داده‌های Market Sheet با استفاده از توکن"""
    headers = {
        "Authorization": f"Bearer {token}",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "fa",
        "Referer": "https://d.easytrader.ir/",
        "Origin": "https://d.easytrader.ir"
    }
    
    try:
        response = requests.get(API_URL_MARKET_SHEET, headers=headers, timeout=10)
        
        if response.status_code == 200:
            return response.json(), False  # موفق، توکن معتبر است
        elif response.status_code == 401:
            print(f"⚠️  خطای 401: توکن منقضی شده!")
            return None, True  # توکن منقضی شده
        else:
            print(f"✗ خطا در Market Sheet API: {response.status_code}")
            return None, False
    except Exception as e:
        print(f"✗ خطا در دریافت Market Sheet: {e}")
        return None, False

def fetch_tsetmc_data():
    """دریافت داده‌های TSETMC (بدون نیاز به توکن)"""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*"
    }
    
    try:
        response = requests.get(API_URL_TSETMC, headers=headers, timeout=10)
        
        if response.status_code == 200:
            return response.json()
        else:
            print(f"✗ خطا در TSETMC API: {response.status_code}")
            return None
    except Exception as e:
        print(f"✗ خطا در دریافت TSETMC: {e}")
        return None

def save_to_database(market_sheet_data, tsetmc_data, engine):
    """ذخیره داده‌ها در دیتابیس SQL Server"""
    try:
        import pandas as pd
        
        timestamp = datetime.now().isoformat()
        fetch_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        df = pd.DataFrame([{
            'timestamp': timestamp,
            'fetch_time': fetch_time,
            'market_sheet_json': json.dumps(market_sheet_data, ensure_ascii=False) if market_sheet_data else None,
            'tsetmc_json': json.dumps(tsetmc_data, ensure_ascii=False) if tsetmc_data else None
        }])
        
        df.to_sql(
            name='market_data',
            con=engine,
            schema=config.TSETMC_SCHEMA,
            if_exists='append',
            index=False,
            method='multi'
        )
        
        print("✓ داده‌ها با موفقیت در دیتابیس ذخیره شدند")
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
    token_refresh_count = 0
    
    try:
        # 1. راه‌اندازی مرورگر و لاگین اولیه
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
            if token.startswith("Bearer "):
                token = token[7:].strip()
                print("✓ 'Bearer' از ابتدای توکن حذف شد")
        
        if not token:
            print("✗ بدون توکن نمی‌توان ادامه داد!")
            return
        
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
        print("⏰ ساعات کاری: فقط بین 11:45 تا 18:00 (ساعت ایران)")
        print("🔄 تمدید خودکار توکن: فعال")
        print("═" * 60 + "\n")
        
        # 2. حلقه دریافت داده
        fetch_count = 0
        error_count = 0
        max_consecutive_errors = 5
        first_save_done = False
        
        while True:
            iran_tz = pytz.timezone('Asia/Tehran')
            now = datetime.now(iran_tz)
            current_hour = now.hour
            current_minute = now.minute
            in_working_hours = (current_hour > 11 or (current_hour == 11 and current_minute >= 45)) and current_hour < 18
            
            timestamp = now.strftime("%Y-%m-%d %H:%M:%S")
            
            if in_working_hours:
                print(f"[{timestamp} IRST] دریافت #{fetch_count + 1}...", end=" ")
                
                # دریافت Market Sheet با چک کردن انقضای توکن
                market_sheet_data, token_expired = fetch_market_sheet_data(token)
                
                # اگر توکن منقضی شده بود
                if token_expired:
                    if token_refresh_count < MAX_TOKEN_REFRESH_ATTEMPTS:
                        new_token, driver, token_refresh_count = refresh_token_if_needed(
                            driver, token, token_refresh_count
                        )
                        
                        if new_token:
                            token = new_token
                            # تلاش مجدد با توکن جدید
                            print(f"[{timestamp} IRST] تلاش مجدد با توکن جدید...", end=" ")
                            market_sheet_data, token_expired = fetch_market_sheet_data(token)
                            
                            if not token_expired and market_sheet_data:
                                error_count = 0  # reset خطاها
                        else:
                            print("❌ تمدید توکن ناموفق بود. ادامه با TSETMC فقط...")
                    else:
                        print(f"❌ حداکثر تلاش ({MAX_TOKEN_REFRESH_ATTEMPTS}) برای تمدید توکن انجام شد")
                        print("⏸️  ادامه فقط با TSETMC...")
                
                # دریافت TSETMC
                tsetmc_data = fetch_tsetmc_data()
                
                # اگر حداقل یکی موفق بود
                if market_sheet_data or tsetmc_data:
                    error_count = 0
                    
                    if save_to_database(market_sheet_data, tsetmc_data, config.engine):
                        fetch_count += 1
                        
                        info_parts = []
                        if market_sheet_data:
                            buy_count = len(market_sheet_data.get('buySheets', []))
                            sell_count = len(market_sheet_data.get('sellSheets', []))
                            info_parts.append(f"MarketSheet(خرید:{buy_count}, فروش:{sell_count})")
                        if tsetmc_data:
                            closing_price = tsetmc_data.get('closingPriceInfo', {}).get('pClosing', 'N/A')
                            info_parts.append(f"TSETMC(قیمت:{closing_price})")
                        
                        print(f"✓ {' + '.join(info_parts)}")
                        
                        # بستن مرورگر بعد از اولین ذخیره موفق
                        if not first_save_done and driver:
                            print("✓ اولین ذخیره موفق. بستن مرورگر...")
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
                
                time.sleep(FETCH_INTERVAL)
            else:
                print(f"[{timestamp} IRST] ⏳ استندبای - انتظار 11:45 (الان: {current_hour:02d}:{current_minute:02d})")
                time.sleep(60)
    
    except KeyboardInterrupt:
        print("\n\n" + "═" * 60)
        print("✓ برنامه توسط کاربر متوقف شد")
        print(f"✓ مجموع {fetch_count} دریافت موفق")
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
            market_sheet_json NVARCHAR(MAX) NULL,
            tsetmc_json NVARCHAR(MAX) NULL,
            created_at DATETIME2 DEFAULT GETDATE()
        )
    END
    ELSE
    BEGIN
        IF NOT EXISTS (SELECT * FROM sys.columns
                       WHERE object_id = OBJECT_ID('tsetmc_api.market_data')
                       AND name = 'tsetmc_json')
        BEGIN
            ALTER TABLE tsetmc_api.market_data
            ADD tsetmc_json NVARCHAR(MAX) NULL
        END
        
        IF EXISTS (SELECT * FROM sys.columns
                   WHERE object_id = OBJECT_ID('tsetmc_api.market_data')
                   AND name = 'data_json')
        BEGIN
            EXEC sp_rename 'tsetmc_api.market_data.data_json', 'market_sheet_json', 'COLUMN'
        END
    END
    """
    
    with engine.connect() as conn:
        conn.execute(text(create_table_sql))
        conn.commit()
    print("✓ جدول market_data ایجاد/به‌روزرسانی شد.")

if __name__ == "__main__":
    create_table_if_not_exists(config.engine)
    main()