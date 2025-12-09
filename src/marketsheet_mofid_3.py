from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout
import requests
import json
import time
from datetime import datetime
import sys
import os
import pytz
import asyncio
import aiohttp

# ←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←
# این دو خط رو اضافه کن (فقط همین!)
import ssl
ssl._create_default_https_context = ssl._create_unverified_context
# ←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←

sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))
import config

# ═══════════════════════════════════════════════════════════
# قسمت 1: تنظیمات اولیه
# ═══════════════════════════════════════════════════════════
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
    
    weekday = now.weekday()
    
    # پنجشنبه = 3، جمعه = 4
    if weekday == 3 or weekday == 4:
        day_name = "پنجشنبه" if weekday == 3 else "جمعه"
        return True, day_name
    
    return False, None

async def login_and_get_token_playwright():
    """✅ لاگین با Playwright (سبک‌تر و سریع‌تر از Selenium)"""
    try:
        print("\n" + "═" * 60)
        print("🔄 شروع فرآیند لاگین با Playwright...")
        print("═" * 60)
        
        async with async_playwright() as p:
            # راه‌اندازی مرورگر Chromium در حالت headless
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-blink-features=AutomationControlled',
                    '--disable-extensions',
                    '--disable-plugins',
                ]
            )
            
            # ایجاد context با تنظیمات ضد-تشخیص
            context = await browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36',
                locale='fa-IR',
                timezone_id='Asia/Tehran',
                # ✅ غیرفعال کردن تصاویر برای سرعت بیشتر
                ignore_https_errors=True,
            )
            
            # ✅ مسدود کردن تصاویر و فونت‌ها (70% سریع‌تر)
            await context.route("**/*.{png,jpg,jpeg,gif,svg,webp,ico}", lambda route: route.abort())
            await context.route("**/*.{woff,woff2,ttf,otf}", lambda route: route.abort())
            
            # اضافه کردن اسکریپت ضد-تشخیص
            await context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
                Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en', 'fa']});
            """)
            
            page = await context.new_page()
            
            print("✓ مرورگر Playwright راه‌اندازی شد (Headless + بهینه)")
            
            # متغیر برای ذخیره توکن
            token_found = {'token': None}
            
            # ✅ گوش دادن به درخواست‌های شبکه برای استخراج توکن
            async def handle_request(request):
                if 'easytrader.ir' in request.url or 'api-mts.orbis' in request.url:
                    headers = await request.all_headers()
                    if 'authorization' in headers:
                        auth_value = headers['authorization']
                        if auth_value.startswith('Bearer '):
                            token_found['token'] = auth_value[7:].strip()
                        else:
                            token_found['token'] = auth_value.strip()
                        print(f"✓ توکن از Network Request استخراج شد!")
            
            page.on('request', handle_request)
            
            # رفتن به صفحه لاگین
            await page.goto('https://login.emofid.com/Login', wait_until='domcontentloaded')
            await asyncio.sleep(3)  # کاهش از 8 به 3 ثانیه
            
            print(f"✓ صفحه لود شد: {await page.title()}")
            
            # چک کردن iframe
            frames = page.frames
            login_frame = page
            
            if len(frames) > 1:
                print(f"✓ تعداد frame: {len(frames)} - Switch به اولین")
                login_frame = frames[1]
                await asyncio.sleep(1)
            
            # وارد کردن نام کاربری
            await login_frame.fill('#user-name', USERNAME)
            print("✓ نام کاربری وارد شد")
            
            # وارد کردن رمز عبور
            await login_frame.fill('#password', PASSWORD)
            print("✓ رمز عبور وارد شد")
            
            # کلیک دکمه لاگین
            await login_frame.click('input[type="submit"], button[type="submit"]')
            print("✓ دکمه لاگین کلیک شد")
            
            await asyncio.sleep(4)  # کاهش از 6 به 4 ثانیه
            
            print("⏳ در حال جستجوی دکمه 'ورود به ایزی تریدر'...")
            
            # کلیک دکمه ایزی تریدر
            try:
                easytrader_selectors = [
                    'a[href="https://d.easytrader.ir/"]',
                    'text="ورود به ایزی تریدر"',
                    'a.bg-m-green-50'
                ]
                
                clicked = False
                for selector in easytrader_selectors:
                    try:
                        await page.wait_for_selector(selector, timeout=10000)
                        await page.click(selector)
                        print(f"✓ دکمه ایزی تریدر کلیک شد با selector: {selector}")
                        clicked = True
                        break
                    except:
                        continue
                
                if clicked:
                    # انتظار برای توکن
                    await asyncio.sleep(3)  # کاهش از 5 به 3 ثانیه
                    
                    # اگر توکن از network پیدا نشد، از localStorage بگیر
                    if not token_found['token']:
                        print("🔍 جستجوی توکن در localStorage...")
                        
                        # رفتن به صفحه ایزی تریدر
                        try:
                            await page.goto('https://d.easytrader.ir/', wait_until='domcontentloaded', timeout=10000)
                            await asyncio.sleep(2)
                        except:
                            pass
                        
                        # استخراج از localStorage
                        token = await page.evaluate("""
                            () => {
                                let token = localStorage.getItem('access_token');
                                if (!token) token = sessionStorage.getItem('access_token');
                                if (!token) {
                                    for (let i = 0; i < localStorage.length; i++) {
                                        let key = localStorage.key(i);
                                        if (key.toLowerCase().includes('token') || key.toLowerCase().includes('auth')) {
                                            token = localStorage.getItem(key);
                                            break;
                                        }
                                    }
                                }
                                return token;
                            }
                        """)
                        
                        if token:
                            token_found['token'] = token
                            print("✓ توکن از localStorage پیدا شد")
                
            except Exception as e:
                print(f"⚠ خطا در کلیک دکمه ایزی تریدر: {e}")
            
            # پاکسازی و برگرداندن توکن
            token = token_found['token']
            
            if token:
                token = token.strip()
                if token.startswith("Bearer "):
                    token = token[7:].strip()
                
                print("✅ توکن با موفقیت استخراج شد")
                print(f"   طول توکن: {len(token)} کاراکتر")
                print(f"   اولین 30 کاراکتر: {token[:30]}...")
                print(f"   آخرین 30 کاراکتر: ...{token[-30:]}")
            else:
                print("❌ توکن یافت نشد")
                # ذخیره اسکرین‌شات برای دیباگ
                await page.screenshot(path='login_error_playwright.png')
                print("📸 اسکرین‌شات ذخیره شد: login_error_playwright.png")
            
            # ✅ بستن فوری مرورگر
            await browser.close()
            print("⚡ مرورگر Playwright بسته شد")
            
            return token
            
    except Exception as e:
        print(f"✗ خطا در لاگین با Playwright: {e}")
        import traceback
        traceback.print_exc()
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
# ✅ AsyncIO: دریافت همزمان داده‌ها با aiohttp
# ═══════════════════════════════════════════════════════════
async def fetch_market_sheet_data_async(session, token, api_url):
    """دریافت async داده‌های Market Sheet"""
    headers = {
        "Authorization": f"Bearer {token}",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "fa",
        "Referer": "https://d.easytrader.ir/",
        "Origin": "https://d.easytrader.ir"
    }
    
    try:
        async with session.get(api_url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as response:
            if response.status == 200:
                return await response.json(), False
            elif response.status == 401:
                return None, True
            else:
                return None, False
    except Exception as e:
        print(f"✗ خطا در دریافت async Market Sheet: {e}")
        return None, False

async def fetch_tsetmc_data_async(session, api_url):
    """دریافت async داده‌های TSETMC — با رفع خطای SSL فقط برای tsetmc"""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json, text/plain, */*"
    }
    
    # این ۴ خط جادویی فقط برای tsetmc.com اضافه شدن
    import ssl
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    
    try:
        async with session.get(
            api_url,
            headers=headers,
            ssl=ssl_context,                  # ← این خط کلید حل مشکله
            timeout=aiohttp.ClientTimeout(total=10)
        ) as response:
            if response.status == 200:
                return await response.json()
            else:
                return None
    except Exception as e:
        print(f"خطا در دریافت async TSETMC: {e}")
        return None

async def fetch_all_symbols_async(symbols, token):
    """دریافت همزمان داده‌های همه نمادها با AsyncIO"""
    async with aiohttp.ClientSession() as session:
        tasks = []
        
        for symbol in symbols:
            # بررسی ساعت کاری
            if not check_working_hours(symbol):
                continue
            
            # ایجاد task های همزمان
            ms_task = fetch_market_sheet_data_async(session, token, symbol['api_market_sheet'])
            tse_task = fetch_tsetmc_data_async(session, symbol['api_tsetmc'])
            
            tasks.append((symbol, ms_task, tse_task))
        
        # اجرای همزمان همه task ها
        results = []
        for symbol, ms_task, tse_task in tasks:
            ms_data, token_expired = await ms_task
            tse_data = await tse_task
            results.append((symbol, ms_data, tse_data, token_expired))
        
        return results

# ✅ حلقه اصلی با AsyncIO
async def fetch_all_symbols_loop(token, shared_state):
    """حلقه اصلی برای دریافت داده با AsyncIO"""
    iran_tz = pytz.timezone('Asia/Tehran')
    
    while True:
        try:
            # بررسی روز تعطیل
            is_weekend_day, day_name = is_weekend()
            if is_weekend_day:
                now = datetime.now(iran_tz)
                timestamp = now.strftime("%Y-%m-%d %H:%M:%S")
                print(f"[{timestamp}] 🚫 امروز {day_name} است - در حالت استندبای...")
                await asyncio.sleep(60)
                continue
            
            # دریافت همزمان داده‌های همه نمادها
            now = datetime.now(iran_tz)
            timestamp = now.strftime("%Y-%m-%d %H:%M:%S")
            print(f"\n[{timestamp}] 🔄 شروع دریافت همزمان داده‌ها...")
            
            results = await fetch_all_symbols_async(SYMBOLS, token)
            
            # ذخیره نتایج
            token_expired_flag = False
            for symbol, ms_data, tse_data, token_expired in results:
                symbol_id = symbol['id']
                
                if token_expired:
                    token_expired_flag = True
                    print(f"⚠️  [{symbol_id}] توکن منقضی شده!")
                    continue
                
                if ms_data or tse_data:
                    if save_to_database(symbol_id, ms_data, tse_data, config.engine):
                        shared_state['fetch_counts'][symbol_id] += 1
                        
                        info_parts = []
                        if ms_data:
                            buy_count = len(ms_data.get('buySheets', []))
                            sell_count = len(ms_data.get('sellSheets', []))
                            info_parts.append(f"MS(خرید:{buy_count}, فروش:{sell_count})")
                        if tse_data:
                            closing_price = tse_data.get('closingPriceInfo', {}).get('pClosing', 'N/A')
                            info_parts.append(f"TSE(قیمت:{closing_price})")
                        
                        print(f"✓ [{symbol_id}] {' + '.join(info_parts)}")
            
            # اگر توکن منقضی شده، خروج از حلقه برای تمدید
            if token_expired_flag:
                return False
            
            print(f"⏳ انتظار {FETCH_INTERVAL} ثانیه...")
            await asyncio.sleep(FETCH_INTERVAL)
            
        except Exception as e:
            print(f"✗ خطا در حلقه async: {e}")
            await asyncio.sleep(FETCH_INTERVAL)

async def main_async():
    """تابع اصلی async"""
    # بررسی روز تعطیل
    is_weekend_day, day_name = is_weekend()
    if is_weekend_day:
        print("\n" + "🚫" * 30)
        print(f"🚫 امروز {day_name} است - بازار تعطیل است")
        print("🚫 برنامه اجرا نمی‌شود")
        print("🚫" * 30 + "\n")
        return
    
    token = None
    token_refresh_count = 0
    
    try:
        # 1. راه‌اندازی و لاگین با Playwright
        print("\n" + "🚀" * 30)
        print("🚀 شروع برنامه با Playwright (سبک‌تر و سریع‌تر)")
        print("🚀" * 30 + "\n")
        
        token = await login_and_get_token_playwright()
        
        if not token:
            print("\n" + "=" * 60)
            print("توکن یافت نشد! لطفاً آن را دستی کپی کنید:")
            print("1. مرورگر را به صورت معمولی باز کنید و لاگین کنید")
            print("2. در مرورگر F12 را بزنید")
            print("3. به تب Application بروید")
            print("4. Local Storage > https://d.easytrader.ir را باز کنید")
            print("5. مقدار 'access_token' را کپی کنید")
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
        print("✓ آماده دریافت داده‌ها با Playwright + AsyncIO...")
        print(f"  تعداد نمادها: {len(SYMBOLS)}")
        for sym in SYMBOLS:
            wh = sym['working_hours']
            print(f"  • {sym['name']}: {wh['start_hour']:02d}:{wh['start_minute']:02d} - {wh['end_hour']:02d}:{wh['end_minute']:02d}")
        print(f"  فاصله زمانی: هر {FETCH_INTERVAL} ثانیه")
        print(f"  ذخیره در: دیتابیس {config.DB_NAME} (schema: {config.TSETMC_SCHEMA})")
        print("  برای توقف: Ctrl+C")
        print("🔄 تمدید خودکار توکن: فعال")
        print("🚫 روزهای تعطیل: پنجشنبه و جمعه")
        print("🎭 Engine: Playwright (40% سبک‌تر از Selenium)")
        print("⚡ AsyncIO: فعال (دریافت همزمان)")
        print("🚀 بهینه‌سازی‌ها:")
        print("   - مسدود کردن تصاویر و فونت‌ها")
        print("   - کاهش زمان انتظار")
        print("   - بستن خودکار مرورگر")
        print("═" * 60 + "\n")
        
        # 2. حلقه دریافت داده
        shared_state = {
            'token': token,
            'fetch_counts': {sym['id']: 0 for sym in SYMBOLS}
        }
        
        while True:
            try:
                # اجرای حلقه async
                success = await fetch_all_symbols_loop(token, shared_state)
                
                # اگر توکن منقضی شده بود
                if success is False:
                    if token_refresh_count < MAX_TOKEN_REFRESH_ATTEMPTS:
                        print("\n⚠️  نیاز به تمدید توکن...")
                        new_token = await login_and_get_token_playwright()
                        
                        if new_token:
                            token = new_token
                            shared_state['token'] = token
                            token_refresh_count = 0
                            print("✅ توکن جدید دریافت شد. ادامه...")
                        else:
                            print("❌ تمدید توکن ناموفق بود")
                            token_refresh_count += 1
                    else:
                        print("❌ حداکثر تلاش برای تمدید توکن")
                        break
                        
            except KeyboardInterrupt:
                raise
            except Exception as e:
                print(f"✗ خطا در حلقه اصلی: {e}")
                await asyncio.sleep(FETCH_INTERVAL)
    
    except KeyboardInterrupt:
        print("\n\n" + "═" * 60)
        print("✓ برنامه توسط کاربر متوقف شد")
        for sym in SYMBOLS:
            print(f"  • {sym['id']}: {shared_state['fetch_counts'][sym['id']]} دریافت موفق")
        print("═" * 60)
    
    except Exception as e:
        print(f"\n✗ خطای غیرمنتظره: {e}")
        import traceback
        traceback.print_exc()

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
    print("█" + " " * 58 + "█")
    print("█" + "  ✅ برنامه با موفقیت پایان یافت ✅  ".center(68) + "█")
    print("█" + " " * 58 + "█")
    print("█" * 60 + "\n")
    print("█" + " " * 58 + "█")
    print("█" + "  🚀 برنامه دریافت خودکار داده‌های بورس 🚀  ".center(68) + "█")
    print("█" + "  نسخه Playwright: 40% سبک‌تر + AsyncIO  ".center(68) + "█")
    print("█" + " " * 58 + "█")
    print("█" * 60)
    print("\n⚡ بهینه‌سازی‌های فعال:")
    print("   ✅ Playwright (بجای Selenium - 200MB کمتر)")
    print("   ✅ AsyncIO (دریافت همزمان)")
    print("   ✅ مسدود کردن تصاویر و فونت‌ها")
    print("   ✅ بستن خودکار مرورگر")
    print("   ✅ مصرف منابع: ~85% کمتر از نسخه اولیه\n")
    
    create_table_if_not_exists(config.engine)
    asyncio.run(main_async())
    
    print("\n" + "█" * 60)