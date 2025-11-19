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
import threading

sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))
import config

# ═══════════════════════════════════════════════════════════
# قسمت 1: تنظیمات اولیه
# ═══════════════════════════════════════════════════════════
DRIVER_PATH = r'D:\web_scraping\web_scraping_course\chromedriver-win64\chromedriver.exe'
USERNAME = '0062980920'
PASSWORD = 'Ard136011@@'

# تنظیمات نمادها
SYMBOLS = [
    {
        'id': 'IRTKGANJ',
        'name': 'IRTKGANJ0001',
        'api_market_sheet': 'https://api-mts.orbis.easytrader.ir/ms/api/MarketSheet/all/IRTKGANJ0001',
        'api_tsetmc': 'https://cdn.tsetmc.com/api/ClosingPrice/GetClosingPriceInfo/58514988269776425',
        'working_hours': {
            'start_hour': 11,
            'start_minute': 45,
            'end_hour': 18,
            'end_minute': 0
        }
    },
    {
        'id': 'IRT3MOJF',
        'name': 'IRT3MOJF0001',
        'api_market_sheet': 'https://api-mts.orbis.easytrader.ir/ms/api/MarketSheet/all/IRT3MOJF0001',
        'api_tsetmc': 'https://cdn.tsetmc.com/api/ClosingPrice/GetClosingPriceInfo/67141987086032267',
        'working_hours': {
            'start_hour': 8,
            'start_minute': 45,
            'end_hour': 12,
            'end_minute': 30
        }
    }
]

FETCH_INTERVAL = 5
MAX_TOKEN_REFRESH_ATTEMPTS = 3

# ═══════════════════════════════════════════════════════════
# قسمت 2: توابع کمکی
# ═══════════════════════════════════════════════════════════
def is_weekend():
    """بررسی اینکه آیا امروز پنجشنبه یا جمعه است (تعطیلات آخر هفته)"""
    iran_tz = pytz.timezone('Asia/Tehran')
    now = datetime.now(iran_tz)
    
    # weekday() برمی‌گرداند: 0=دوشنبه، 1=سه‌شنبه، 2=چهارشنبه، 3=پنجشنبه، 4=جمعه، 5=شنبه، 6=یکشنبه
    weekday = now.weekday()
    
    # پنجشنبه = 3، جمعه = 4
    if weekday == 3 or weekday == 4:
        day_name = "پنجشنبه" if weekday == 3 else "جمعه"
        return True, day_name
    
    return False, None

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

def fetch_market_sheet_data(token, api_url):
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
        response = requests.get(api_url, headers=headers, timeout=10)
        
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

def fetch_tsetmc_data(api_url):
    """دریافت داده‌های TSETMC (بدون نیاز به توکن)"""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*"
    }
    
    try:
        response = requests.get(api_url, headers=headers, timeout=10)
        
        if response.status_code == 200:
            return response.json()
        else:
            print(f"✗ خطا در TSETMC API: {response.status_code}")
            return None
    except Exception as e:
        print(f"✗ خطا در دریافت TSETMC: {e}")
        return None

def save_to_database(symbol_id, market_sheet_data, tsetmc_data, engine):
    """ذخیره داده‌ها در دیتابیس SQL Server"""
    try:
        import pandas as pd
        
        timestamp = datetime.now().isoformat()
        fetch_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        df = pd.DataFrame([{
            'symbol_id': symbol_id,
            'timestamp': timestamp,
            'fetch_time': fetch_time,
            'market_sheet_json': json.dumps(market_sheet_data, ensure_ascii=False) if market_sheet_data else None,
            'tsetmc_json': json.dumps(tsetmc_data, ensure_ascii=False) if tsetmc_data else None
        }])
        
        df.to_sql(
            name='market_data_combined',
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

def check_working_hours(symbol):
    """بررسی اینکه آیا در ساعت کاری نماد هستیم"""
    iran_tz = pytz.timezone('Asia/Tehran')
    now = datetime.now(iran_tz)
    current_hour = now.hour
    current_minute = now.minute
    
    wh = symbol['working_hours']
    start_hour = wh['start_hour']
    start_minute = wh['start_minute']
    end_hour = wh['end_hour']
    end_minute = wh['end_minute']
    
    # بررسی ساعت شروع
    if current_hour < start_hour or (current_hour == start_hour and current_minute < start_minute):
        return False
    
    # بررسی ساعت پایان
    if current_hour > end_hour or (current_hour == end_hour and current_minute > end_minute):
        return False
    
    return True

# ═══════════════════════════════════════════════════════════
# قسمت 3: برنامه اصلی برای هر نماد
# ═══════════════════════════════════════════════════════════
def fetch_symbol_data(symbol, token, driver, token_refresh_count, shared_state):
    """دریافت داده برای یک نماد"""
    symbol_id = symbol['id']
    iran_tz = pytz.timezone('Asia/Tehran')
    now = datetime.now(iran_tz)
    timestamp = now.strftime("%Y-%m-%d %H:%M:%S")
    
    if not check_working_hours(symbol):
        wh = symbol['working_hours']
        print(f"[{timestamp} - {symbol_id}] ⏳ استندبای - خارج از ساعت کاری ({wh['start_hour']:02d}:{wh['start_minute']:02d}-{wh['end_hour']:02d}:{wh['end_minute']:02d})")
        return token, driver, token_refresh_count, False
    
    print(f"[{timestamp} - {symbol_id}] دریافت #{shared_state['fetch_counts'][symbol_id] + 1}...", end=" ")
    
    # دریافت Market Sheet
    market_sheet_data, token_expired = fetch_market_sheet_data(token, symbol['api_market_sheet'])
    
    # اگر توکن منقضی شده بود
    if token_expired:
        if token_refresh_count < MAX_TOKEN_REFRESH_ATTEMPTS:
            new_token, driver, token_refresh_count = refresh_token_if_needed(
                driver, token, token_refresh_count
            )
            
            if new_token:
                token = new_token
                shared_state['token'] = token  # به‌روزرسانی توکن مشترک
                # تلاش مجدد با توکن جدید
                print(f"[{timestamp} - {symbol_id}] تلاش مجدد با توکن جدید...", end=" ")
                market_sheet_data, token_expired = fetch_market_sheet_data(token, symbol['api_market_sheet'])
            else:
                print(f"❌ [{symbol_id}] تمدید توکن ناموفق بود")
        else:
            print(f"❌ [{symbol_id}] حداکثر تلاش برای تمدید توکن")
    
    # دریافت TSETMC
    tsetmc_data = fetch_tsetmc_data(symbol['api_tsetmc'])
    
    # اگر حداقل یکی موفق بود
    if market_sheet_data or tsetmc_data:
        if save_to_database(symbol_id, market_sheet_data, tsetmc_data, config.engine):
            shared_state['fetch_counts'][symbol_id] += 1
            
            info_parts = []
            if market_sheet_data:
                buy_count = len(market_sheet_data.get('buySheets', []))
                sell_count = len(market_sheet_data.get('sellSheets', []))
                info_parts.append(f"MS(خرید:{buy_count}, فروش:{sell_count})")
            if tsetmc_data:
                closing_price = tsetmc_data.get('closingPriceInfo', {}).get('pClosing', 'N/A')
                info_parts.append(f"TSE(قیمت:{closing_price})")
            
            print(f"✓ {' + '.join(info_parts)}")
            return token, driver, token_refresh_count, True
        else:
            print("✗ خطا در ذخیره")
    else:
        print(f"✗ دریافت ناموفق")
    
    return token, driver, token_refresh_count, False

def main():
    # بررسی روز تعطیل
    is_weekend_day, day_name = is_weekend()
    if is_weekend_day:
        print("\n" + "🚫" * 30)
        print(f"🚫 امروز {day_name} است - بازار تعطیل است")
        print("🚫 برنامه اجرا نمی‌شود")
        print("🚫" * 30 + "\n")
        return
    
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
        print("✓ آماده دریافت داده‌ها برای چند نماد...")
        print(f"  تعداد نمادها: {len(SYMBOLS)}")
        for sym in SYMBOLS:
            wh = sym['working_hours']
            print(f"  • {sym['name']}: {wh['start_hour']:02d}:{wh['start_minute']:02d} - {wh['end_hour']:02d}:{wh['end_minute']:02d}")
        print(f"  فاصله زمانی: هر {FETCH_INTERVAL} ثانیه")
        print(f"  ذخیره در: دیتابیس {config.DB_NAME} (schema: {config.TSETMC_SCHEMA})")
        print("  برای توقف: Ctrl+C")
        print("🔄 تمدید خودکار توکن: فعال")
        print("🚫 روزهای تعطیل: پنجشنبه و جمعه")
        print("═" * 60 + "\n")
        
        # 2. حلقه دریافت داده
        shared_state = {
            'token': token,
            'fetch_counts': {sym['id']: 0 for sym in SYMBOLS}
        }
        first_save_done = False
        
        while True:
            # بررسی مجدد روز تعطیل در هر iteration
            is_weekend_day, day_name = is_weekend()
            if is_weekend_day:
                iran_tz = pytz.timezone('Asia/Tehran')
                now = datetime.now(iran_tz)
                timestamp = now.strftime("%Y-%m-%d %H:%M:%S")
                print(f"[{timestamp}] 🚫 امروز {day_name} است - در حالت استندبای...")
                time.sleep(60)  # چک کردن هر 1 دقیقه
                continue
            
            # دریافت داده برای همه نمادها
            for symbol in SYMBOLS:
                token, driver, token_refresh_count, success = fetch_symbol_data(
                    symbol, token, driver, token_refresh_count, shared_state
                )
                
                # بستن مرورگر بعد از اولین ذخیره موفق
                if success and not first_save_done and driver:
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
            
            time.sleep(FETCH_INTERVAL)
    
    except KeyboardInterrupt:
        print("\n\n" + "═" * 60)
        print("✓ برنامه توسط کاربر متوقف شد")
        for sym in SYMBOLS:
            print(f"  • {sym['id']}: {shared_state['fetch_counts'][sym['id']]} دریافت موفق")
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
    """ایجاد جدول market_data_combined اگر وجود نداشته باشد"""
    from sqlalchemy import text
    
    create_table_sql = """
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='market_data_combined' AND xtype='U')
    BEGIN
        CREATE TABLE tsetmc_api.market_data_combined (
            id INT IDENTITY(1,1) PRIMARY KEY,
            symbol_id NVARCHAR(50) NOT NULL,
            timestamp DATETIME2 NOT NULL,
            fetch_time NVARCHAR(50) NOT NULL,
            market_sheet_json NVARCHAR(MAX) NULL,
            tsetmc_json NVARCHAR(MAX) NULL,
            created_at DATETIME2 DEFAULT GETDATE()
        )
        
        -- ایجاد ایندکس برای symbol_id
        CREATE INDEX IX_market_data_combined_symbol_id ON tsetmc_api.market_data_combined(symbol_id)
        CREATE INDEX IX_market_data_combined_timestamp ON tsetmc_api.market_data_combined(timestamp)
    END
    ELSE
    BEGIN
        -- اطمینان از وجود ستون symbol_id
        IF NOT EXISTS (SELECT * FROM sys.columns
                       WHERE object_id = OBJECT_ID('tsetmc_api.market_data_combined')
                       AND name = 'symbol_id')
        BEGIN
            ALTER TABLE tsetmc_api.market_data_combined
            ADD symbol_id NVARCHAR(50) NOT NULL DEFAULT 'UNKNOWN'
        END
        
        IF NOT EXISTS (SELECT * FROM sys.columns
                       WHERE object_id = OBJECT_ID('tsetmc_api.market_data_combined')
                       AND name = 'tsetmc_json')
        BEGIN
            ALTER TABLE tsetmc_api.market_data_combined
            ADD tsetmc_json NVARCHAR(MAX) NULL
        END
    END
    """
    
    with engine.connect() as conn:
        conn.execute(text(create_table_sql))
        conn.commit()
    print("✓ جدول market_data_combined ایجاد/به‌روزرسانی شد.")

if __name__ == "__main__":
    create_table_if_not_exists(config.engine)
    main()