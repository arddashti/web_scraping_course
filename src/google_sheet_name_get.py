import os
import json
from google.oauth2 import service_account
from googleapiclient.discovery import build

# مسیر فایل کلید سرویس
SERVICE_ACCOUNT_FILE = 'C:/Users/a.dashti.PEDC/Downloads/key/n8n-api-458619-03c59882eb51.json'

# شناسه شیت مورد نظر
SPREADSHEET_ID = '12ZncYRyAJqdBYbn6X2gdh4IGFhD-sdDntkyL4UCfM8E'

# دامنه‌های دسترسی
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

# احراز هویت با استفاده از کلید سرویس
credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES)

# ساخت سرویس Google Sheets API
service = build('sheets', 'v4', credentials=credentials)

# دریافت متادیتای شیت
sheet_metadata = service.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()

# استخراج نام شیت‌ها
sheet_names = [sheet['properties']['title'] for sheet in sheet_metadata['sheets']]

# نمایش نام شیت‌ها
print("نام شیت‌ها:")
for name in sheet_names:
    print(name)
