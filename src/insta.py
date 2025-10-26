import requests
import json
import time
import random

# --- تنظیمات ---
BASE_URL = "https://www.instagram.com/api/v1/friendships/70343962129/followers/"
COUNT = 12  # Instagram max per page (برای followers معمولاً 12-50)
HEADERS = {
    "accept": "*/*",
    "accept-language": "en-US,en;q=0.9,fa;q=0.8",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36",
    "x-asbd-id": "359341",
    "x-csrftoken": "sOp8ELFawBxRfxghRS0_aQ",  # بروزرسانی کن
    "x-ig-app-id": "936619743392459",
    "x-requested-with": "XMLHttpRequest",
    "referer": "https://www.instagram.com/honcha_issyk/followers/",
}
COOKIES = {
    "csrftoken": "sOp8ELFawBxRfxghRS0_aQ",
    "sessionid": "1604751465%3AOPDskmHDu23Npr%3A28%3AAYghopQ8roVp378lckZ8FBq_Wv1kNHfz3sCxbqrktw",
    "ds_user_id": "1604751465",
}

def fetch_page(max_id=None):
    params = {"count": COUNT, "search_surface": "follow_list_page"}
    if max_id:
        params["max_id"] = max_id
    
    url = BASE_URL + "?" + "&".join(f"{k}={v}" for k, v in params.items())
    
    for attempt in range(3):
        try:
            r = requests.get(url, headers=HEADERS, cookies=COOKIES, timeout=15)
            if r.status_code != 200:
                print(f"⚠️ HTTP {r.status_code} در تلاش {attempt+1}")
                time.sleep(random.uniform(2, 5))
                continue
            
            # مدیریت ZSTD یا JSON مستقیم
            try:
                data = r.json()
            except ValueError:
                encoding = r.headers.get("content-encoding", "")
                if "zstd" in encoding.lower():
                    import zstandard as zstd
                    decompressor = zstd.ZstdDecompressor()
                    raw = decompressor.decompress(r.content)
                    data = json.loads(raw.decode("utf-8"))
                else:
                    print("❌ محتوای نامعتبر:", r.text[:500])
                    continue
            
            if data.get("status") != "ok":
                print("❌ API error:", data)
                continue
            
            return data["users"], data.get("next_max_id")
            
        except requests.RequestException as e:
            print(f"⚠️ خطای شبکه در تلاش {attempt+1}: {e}")
            time.sleep(random.uniform(2, 5))
    
    return None, None

# --- تابع اصلی: همه followers ---
def fetch_all_followers():
    all_users = []
    max_id = None  # برای صفحه اول None
    page = 1
    
    while True:
        print(f"📄 صفحه {page} - max_id: {max_id or 'شروع'}")
        users, next_max_id = fetch_page(max_id)
        
        if not users:
            print("🚫 شکست در دریافت صفحه")
            break
        
        all_users.extend(users)
        print(f"✅ {len(users)} کاربر جدید - مجموع: {len(all_users)}")
        
        # ذخیره تدریجی
        with open("followers_partial.jsonl", "a", encoding="utf-8") as f:
            for user in users:
                f.write(json.dumps(user, ensure_ascii=False) + "\n")
        
        if not next_max_id or len(users) < COUNT:
            print("🏁 همه followers دریافت شد!")
            break
        
        max_id = next_max_id
        page += 1
        
        # تاخیر برای rate limit
        delay = random.uniform(3, 6)
        print(f"⏳ انتظار {delay:.1f} ثانیه...")
        time.sleep(delay)
    
    return all_users

# --- اجرا ---
if __name__ == "__main__":
    followers = fetch_all_followers()
    
    # ذخیره نهایی به عنوان JSON array
    with open("followers.json", "w", encoding="utf-8") as f:
        json.dump(followers, f, ensure_ascii=False, indent=2)
    
    print(f"💾 ذخیره کامل: {len(followers)} follower در followers.json")