from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import time

chrome_options = Options()
chrome_options.add_argument("--start-maximized")

# webdriver_manager خودش ChromeDriver مناسب را دانلود و اجرا میکند
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

url = "https://rahavard365.com/asset/26054/profitloss"
driver.get(url)

time.sleep(5)
print("صفحه باز شد:", driver.title)

input("برای بستن مرورگر Enter را بزنید...")
driver.quit()

