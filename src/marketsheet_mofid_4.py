from playwright.async_api import async_playwright
import aiohttp
import asyncio
import json
import time
from datetime import datetime
import pytz
from collections import deque
from sqlalchemy import create_engine, Table, MetaData, Column, Integer, String, DateTime
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.pool import QueuePool
import config

# ═══════════════════════════════════════════════
# تنظیمات
# ═══════════════════════════════════════════════
USERNAME = '0062980920'
PASSWORD = 'Ard136011@@'

SYMBOLS = [
    {
        'id': 'IRTKGANJ',
        'name': 'IRTKGANJ0001',
        'api_market_sheet': 'https://api-mts.orbis.easytrader.ir/ms/api/MarketSheet/all/IRTKGANJ0001',
        'api_tsetmc': 'https://cdn.tsetmc.com/api/ClosingPrice/GetClosingPriceInfo/58514988269776425',
        'working_hours': {'start_hour':11,'start_minute':45,'end_hour':18,'end_minute':0}
    },
    {
        'id': 'IRT3MOJF',
        'name': 'IRT3MOJF0001',
        'api_market_sheet': 'https://api-mts.orbis.easytrader.ir/ms/api/MarketSheet/all/IRT3MOJF0001',
        'api_tsetmc': 'https://cdn.tsetmc.com/api/ClosingPrice/GetClosingPriceInfo/67141987086032267',
        'working_hours': {'start_hour':8,'start_minute':45,'end_hour':12,'end_minute':30}
    }
]

FETCH_INTERVAL = 5
BATCH_SIZE = 50
BATCH_TIMEOUT = 30
MAX_TOKEN_REFRESH_ATTEMPTS = 3

# ═══════════════════════════════════════════════
# اتصال به دیتابیس
# ═══════════════════════════════════════════════
def create_db_engine_with_pool():
    connection_string = f"mssql+pyodbc://{config.DB_USER}:{config.DB_PASS}@{config.DB_SERVER}/{config.DB_NAME}?driver=ODBC+Driver+17+for+SQL+Server"
    engine = create_engine(
        connection_string,
        poolclass=QueuePool,
        pool_size=10,
        max_overflow=20,
        pool_timeout=30,
        pool_recycle=3600,
        pool_pre_ping=True,
        echo=False
    )
    return engine

db_engine = create_db_engine_with_pool()
SessionFactory = scoped_session(sessionmaker(bind=db_engine))

# ═══════════════════════════════════════════════
# Batch buffer
# ═══════════════════════════════════════════════
batch_buffer = deque()
batch_lock = asyncio.Lock()
last_batch_time = time.time()

# ═══════════════════════════════════════════════
# توابع کمکی
# ═══════════════════════════════════════════════
def is_weekend():
    iran_tz = pytz.timezone('Asia/Tehran')
    now = datetime.now(iran_tz)
    weekday = now.weekday()
    if weekday in (3,4):
        return True, "پنجشنبه" if weekday==3 else "جمعه"
    return False, None

def check_working_hours(symbol):
    iran_tz = pytz.timezone('Asia/Tehran')
    now = datetime.now(iran_tz)
    wh = symbol['working_hours']
    sh, sm = wh['start_hour'], wh['start_minute']
    eh, em = wh['end_hour'], wh['end_minute']
    if now.hour < sh or (now.hour==sh and now.minute<sm): return False
    if now.hour > eh or (now.hour==eh and now.minute>em): return False
    return True

# ═══════════════════════════════════════════════
# Playwright login
# ═══════════════════════════════════════════════
async def login_and_get_token_playwright():
    token_found = {'token': None}
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=[
                '--no-sandbox','--disable-setuid-sandbox','--disable-dev-shm-usage',
                '--disable-blink-features=AutomationControlled','--disable-extensions','--disable-plugins'
            ])
            context = await browser.new_context(
                viewport={'width':1920,'height':1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
                locale='fa-IR', timezone_id='Asia/Tehran', ignore_https_errors=True
            )
            await context.route("**/*.{png,jpg,jpeg,gif,svg,webp,ico}", lambda route: route.abort())
            await context.route("**/*.{woff,woff2,ttf,otf}", lambda route: route.abort())
            page = await context.new_page()

            async def handle_request(request):
                headers = await request.all_headers()
                auth = headers.get('authorization')
                if auth:
                    token_found['token'] = auth.replace('Bearer ','').strip()
            page.on('request', handle_request)

            await page.goto('https://login.emofid.com/Login', wait_until='domcontentloaded')
            await asyncio.sleep(2)
            frames = page.frames
            login_frame = frames[1] if len(frames)>1 else page
            await login_frame.fill('#user-name', USERNAME)
            await login_frame.fill('#password', PASSWORD)
            await login_frame.click('input[type="submit"], button[type="submit"]')
            await asyncio.sleep(3)
            # Click easytrader
            try:
                await page.click('text="ورود به ایزی تریدر"', timeout=5000)
            except: pass
            await asyncio.sleep(2)
            # fallback localStorage
            if not token_found['token']:
                token = await page.evaluate("""() => {
                    return localStorage.getItem('access_token') || sessionStorage.getItem('access_token')
                }""")
                token_found['token'] = token
            await browser.close()
    except Exception as e:
        print(f"Login error: {e}")
    return token_found['token']

# ═══════════════════════════════════════════════
# Batch Insert
# ═══════════════════════════════════════════════
async def add_to_batch(symbol_id, market_sheet_data, tsetmc_data):
    global last_batch_time
    record = {
        'symbol_id': symbol_id,
        'timestamp': datetime.now(),
        'fetch_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'market_sheet_json': json.dumps(market_sheet_data, ensure_ascii=False) if market_sheet_data else None,
        'tsetmc_json': json.dumps(tsetmc_data, ensure_ascii=False) if tsetmc_data else None
    }
    async with batch_lock:
        batch_buffer.append(record)
        if len(batch_buffer) >= BATCH_SIZE or (time.time()-last_batch_time)>=BATCH_TIMEOUT:
            await flush_batch()
            last_batch_time = time.time()

async def flush_batch(force=False):
    global batch_buffer
    async with batch_lock:
        if not batch_buffer and not force: return
        records = list(batch_buffer)
        batch_buffer.clear()
    session = SessionFactory()
    metadata = MetaData()
    market_data_table = Table(
        'market_data_combined', metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('symbol_id', String(50)),
        Column('timestamp', DateTime),
        Column('fetch_time', String(50)),
        Column('market_sheet_json', String),
        Column('tsetmc_json', String),
        Column('created_at', DateTime, default=datetime.now),
        schema=config.TSETMC_SCHEMA
    )
    try:
        with session.begin():
            session.execute(market_data_table.insert(), records)
        print(f"✅ Batch Insert: {len(records)} رکورد ذخیره شد")
    except Exception as e:
        print(f"Batch insert error: {e}")
    finally:
        session.close()

# ═══════════════════════════════════════════════
# Fetch async
# ═══════════════════════════════════════════════
async def fetch_market_sheet_data_async(session, token, api_url):
    headers = {"Authorization":f"Bearer {token}","User-Agent":"Mozilla/5.0","Accept":"application/json"}
    try:
        async with session.get(api_url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status==200: return await resp.json(), False
            if resp.status==401: return None, True
            return None, False
    except: return None, False

async def fetch_tsetmc_data_async(session, api_url):
    headers = {"User-Agent":"Mozilla/5.0","Accept":"application/json"}
    try:
        async with session.get(api_url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status==200: return await resp.json()
            return None
    except: return None

async def fetch_symbol(symbol, token, session):
    ms_data, token_expired = await fetch_market_sheet_data_async(session, token, symbol['api_market_sheet'])
    tse_data = await fetch_tsetmc_data_async(session, symbol['api_tsetmc'])
    return symbol, ms_data, tse_data, token_expired

async def fetch_all_symbols_async(symbols, token):
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_symbol(sym, token, session) for sym in symbols if check_working_hours(sym)]
        results = await asyncio.gather(*tasks)
        return results

# ═══════════════════════════════════════════════
# Main loop
# ═══════════════════════════════════════════════
async def fetch_all_symbols_loop(token, shared_state):
    global last_batch_time
    async def auto_flush_batch():
        while True:
            await asyncio.sleep(BATCH_TIMEOUT)
            async with batch_lock:
                if batch_buffer and (time.time()-last_batch_time)>=BATCH_TIMEOUT:
                    await flush_batch()
                    last_batch_time = time.time()
    flush_task = asyncio.create_task(auto_flush_batch())
    try:
        while True:
            is_weekend_day, day_name = is_weekend()
            if is_weekend_day:
                await flush_batch(force=True)
                await asyncio.sleep(60)
                continue
            results = await fetch_all_symbols_async(SYMBOLS, token)
            token_expired_flag = False
            for symbol, ms_data, tse_data, token_expired in results:
                if token_expired: token_expired_flag = True; continue
                await add_to_batch(symbol['id'], ms_data, tse_data)
                shared_state['fetch_counts'][symbol['id']] += 1
            if token_expired_flag:
                await flush_batch(force=True)
                return False
            await asyncio.sleep(FETCH_INTERVAL)
    finally:
        flush_task.cancel()
        try: await flush_task
        except asyncio.CancelledError: pass

# ═══════════════════════════════════════════════
# Main async
# ═══════════════════════════════════════════════
async def main_async():
    is_weekend_day, day_name = is_weekend()
    if is_weekend_day:
        print(f"🚫 امروز {day_name} است - بازار تعطیل")
        return
    token = await login_and_get_token_playwright()
    if not token:
        token = input("توکن را وارد کنید: ").strip()
    shared_state = {'token': token, 'fetch_counts': {s['id']:0 for s in SYMBOLS}}
    token_refresh_count = 0
    while True:
        success = await fetch_all_symbols_loop(token, shared_state)
        if success is False:
            if token_refresh_count < MAX_TOKEN_REFRESH_ATTEMPTS:
                new_token = await login_and_get_token_playwright()
                if new_token:
                    token = new_token
                    shared_state['token'] = token
                    token_refresh_count = 0
                else:
                    token_refresh_count += 1
            else: break

# ═══════════════════════════════════════════════
# Create table if not exists
# ═══════════════════════════════════════════════
def create_table_if_not_exists(engine):
    from sqlalchemy import text
    sql = """
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
        CREATE INDEX IX_market_data_combined_symbol_id ON tsetmc_api.market_data_combined(symbol_id)
        CREATE INDEX IX_market_data_combined_timestamp ON tsetmc_api.market_data_combined(timestamp)
    END
    """
    with engine.connect() as conn:
        conn.execute(text(sql))
        conn.commit()

# ═══════════════════════════════════════════════
# Run
# ═══════════════════════════════════════════════
if __name__ == "__main__":
    create_table_if_not_exists(db_engine)
    asyncio.run(main_async())
    db_engine.dispose()
