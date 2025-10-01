import requests

url = "https://www.nahayatnegar.com/online/v2/api/instrument/IRTKGANJ0001/marketSheet?platform=mobile"

# کوکی‌ها (تنها نمونه، بهتر است کوکی واقعی خودتان را جایگزین کنید)
cookies = {
    "_vid": "1759307224354",
    "__eid": "fpaihntoipei5c3f8r8er0n4jgrv316j",
    "locale_dispatcher": "fa_IR",
    "access_token_neo": "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJpZCI6IjMzMmFhZmI3YzI1M2YwOTFlZmIwMjczZjQ1Zjk2NGRiNTg3YTFkYTUiLCJqdGkiOiIzMzJhYWZiN2MyNTNmMDkxZWZiMDI3M2Y0NWY5NjRkYjU4N2ExZGE1IiwiaXNzIjoiIiwiYXVkIjoiTkVPIiwic3ViIjozNTcxMDgsImV4cCI6MTc1OTMyNTY4OSwiaWF0IjoxNzU5MzA3Njg5LCJ0b2tlbl90eXBlIjoiYmVhcmVyIiwic2NvcGUiOm51bGx9.DoqWuVeiuNywdLINEIO-K07EbBfCG3TdX31Bg6QizKbMAJkk0PO7e5YZ8iZGK3oEmL6BlGxw_tmiW2fQF0A3S9iJtGLycYYjiZxZPf-NNVEwyO5qXhlyrdoqP72EQwvfkHy4IsaqiezPge9NY7fuNCtbq4PXS1AUyEYWv7UFCdLYLAyIl1c8A9NbMbWKckyJBge1PHe0Zoc_ZSFTUqzS5hqOB44ePjxtJ2t2SE1WmgN29XGFyZmwriHzUd9Okvon-3wAQtmpJFPd2JMcHzVBYp4HQdkj1Xz1px8qQIFuHI2DQ61_CPl5nxYDv2sCT9dDEniIJrdhaFKTAxB33M-i_w"
}

# هدرها
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://neotrader.nahayatnegar.com/",
    "Origin": "https://neotrader.nahayatnegar.com"
}

# ارسال درخواست GET
response = requests.get(url, headers=headers, cookies=cookies)

# بررسی وضعیت و چاپ نتیجه
if response.status_code == 200:
    data = response.json()
    print(data)
else:
    print(f"Error: {response.status_code}")
