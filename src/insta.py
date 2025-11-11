import requests
import json
import time
import random

# --- تنظیمات بروزرسانی‌شده بر اساس هدرهای واقعی درخواست موفق ---
USER_ID = "65186258029"
BASE_URL = f"https://www.instagram.com/api/v1/friendships/{USER_ID}/followers/"
COUNT = 50  # افزایش برای سرعت، اما اول با 12 تست کن
HEADERS = {
    "accept": "*/*",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "en-US,en;q=0.9,fa;q=0.8",
    "cookie": "csrftoken=sOp8ELFawBxRfxghRS0_aQ; datr=Qw3-aFv1TG9UYIYF7NQIbd59; ig_did=92FAE599-C88A-4652-B066-9128E6992F84; mid=aP4NQwALAAGT9d-5Q6SpoJW0mAuB; ds_user_id=1604751465; ps_l=1; ps_n=1; sessionid=1604751465%3AOPDskmHDu23Npr%3A28%3AAYgozgP5C6bAA8H4YfU5zNfMM7_8BonAqhoTtBUtBA; wd=932x919; rur=\"LDC\\0541604751465\\0541794407949:01fe018a076410b4a59e3e3885aec1c2e2f619d44e2afb346683e6e506ae1352f7f99861\"",
    "priority": "u=1, i",
    "referer": "https://www.instagram.com/hoztovar_kaz/followers/",
    "sec-ch-prefers-color-scheme": "light",
    "sec-ch-ua": '"Chromium";v="142", "Google Chrome";v="142", "Not_A Brand";v="99"',
    "sec-ch-ua-full-version-list": '"Chromium";v="142.0.7444.134", "Google Chrome";v="142.0.7444.134", "Not_A Brand";v="99.0.0.0"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-model": '""',
    "sec-ch-ua-platform": '"Windows"',
    "sec-ch-ua-platform-version": '"10.0.0"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
    "x-asbd-id": "359341",
    "x-csrftoken": "sOp8ELFawBxRfxghRS0_aQ",
    "x-ig-app-id": "936619743392459",
    "x-ig-www-claim": "hmac.AR1GNZLNUE2Tnx9VSm3s1y_HKkDDYcykhijcj8kUL8YOJVqh",
    "x-requested-with": "XMLHttpRequest",
    "x-web-session-id": "5drtvv:75o9vc:ij7lsg",
}
# نکته: cookie کامل در HEADERS گذاشتم (به جای COOKIES جدا)، چون در درخواست واقعی اینطوریه. اگر خطا داد، به COOKIES منتقل کن.

def fetch_page(max_id=None):
    params = {"count": COUNT, "search_surface": "follow_list_page"}
    if max_id:
        params["max_id"] = max_id
    
    url = BASE_URL + "?" + "&".join(f"{k}={v}" for k, v in params.items())
    
    for attempt in range(5):
        try:
            print(f"🔍 URL: {url}")
            r = requests.get(url, headers=HEADERS, timeout=30)  # timeout افزایش برای دانلود کامل
            print(f"📡 تلاش {attempt+1}: {r.status_code} - Content-Encoding: {r.headers.get('content-encoding', 'none')} - Content-Length: {len(r.content)} bytes")
            
            if r.status_code != 200:
                print(f"⚠️ HTTP {r.status_code} - Response: {r.text[:200]}")
                time.sleep(random.uniform(10, 20))
                continue
            
            # مدیریت ZSTD با بهبود: چک طول محتوا و try-except دقیق‌تر
            encoding = r.headers.get("content-encoding", "").lower()
            if "zstd" in encoding:
                try:
                    import zstandard as zstd
                    decompressor = zstd.ZstdDecompressor()
                    # چک اگر محتوا خالی یا خیلی کوچک
                    if len(r.content) < 10:
                        print("❌ محتوای ZSTD خیلی کوچک - احتمالاً خطای دانلود")
                        continue
                    # استفاده از stream_reader برای هندل بهتر frame header issues
                    with decompressor.stream_reader(r.content) as reader:
                        raw = reader.read()
                    data = json.loads(raw.decode("utf-8"))
                except zstd.ZstdError as ze:
                    print(f"❌ ZSTD Error: {ze} - Raw content hex preview: {r.content[:20].hex()}...")
                    # fallback: اگر frame invalid، سعی کن بدون decompression (نادر)
                    try:
                        data = json.loads(r.content.decode("utf-8", errors="ignore"))
                        print("⚠️ Fallback به raw decode - ممکنه ناقص باشه")
                    except:
                        print("❌ Fallback هم شکست")
                        continue
                except ImportError:
                    print("❌ zstandard نصب نیست: pip install zstandard")
                    continue
                except Exception as e:
                    print(f"❌ ZSTD/JSON خطا: {e}")
                    continue
            else:
                # غیر ZSTD: مستقیم JSON
                data = r.json()
            
            if data.get("status") != "ok":
                print(f"❌ API error: {data.get('message', data)}")
                time.sleep(random.uniform(10, 20))
                continue
            
            users = data.get("users", [])
            print(f"✅ OK: {len(users)} کاربر - Next ID: {data.get('next_max_id', 'None')}")
            return users, data.get("next_max_id")
            
        except requests.RequestException as e:
            print(f"⚠️ شبکه خطا: {e}")
            time.sleep(random.uniform(10, 20))
    
    return None, None

# --- تابع اصلی ---
def fetch_all_followers():
    all_users = []
    max_id = None
    page = 1
    total_expected = 4800
    
    # پاک کردن فایل قدیمی
    open("followers_partial.jsonl", "w").close()
    
    while True:
        print(f"\n📄 صفحه {page} - Max ID: {max_id or 'شروع'} - کل: {len(all_users)}/{total_expected}")
        users, next_max_id = fetch_page(max_id)
        
        if not users:
            print("🚫 توقف: کاربرانی دریافت نشد")
            break
        
        all_users.extend(users)
        progress = len(all_users) / total_expected * 100
        print(f"✅ +{len(users)} کاربر - پیشرفت: {progress:.1f}%")
        
        # ذخیره تدریجی (JSONL)
        with open("followers_partial.jsonl", "a", encoding="utf-8") as f:
            for user in users:
                f.write(json.dumps(user, ensure_ascii=False) + "\n")
        
        if not next_max_id or len(users) < COUNT // 2:
            print("🏁 استخراج کامل شد!")
            break
        
        max_id = next_max_id
        page += 1
        
        # تاخیر برای rate limit (بر اساس تجربه، 15-25 ثانیه ایمنه)
        delay = random.uniform(15, 25)
        print(f"⏳ انتظار {delay:.1f} ثانیه...")
        time.sleep(delay)
    
    return all_users

# --- اجرا ---
if __name__ == "__main__":
    print("🚀 شروع استخراج فالوورهای @hoztovar_kaz...")
    followers = fetch_all_followers()
    
    # ذخیره نهایی
    with open("followers_hoztovar_kaz.json", "w", encoding="utf-8") as f:
        json.dump(followers, f, ensure_ascii=False, indent=2)
    
    print(f"\n💾 تمام! {len(followers)} فالوور در followers_hoztovar_kaz.json ذخیره شد.")
    if len(followers) < total_expected * 0.9:
        print("⚠️ ممکنه ناقص باشه - کوکی‌ها رو بروز کن یا تاخیر رو افزایش بده.")