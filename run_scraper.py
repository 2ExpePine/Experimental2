from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from bs4 import BeautifulSoup
import gspread
from datetime import date
import os
import time
import json
import random
from webdriver_manager.chrome import ChromeDriverManager

# ---------------- SHARDING (Logic Unchanged) ---------------- #
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_STEP = int(os.getenv("SHARD_STEP", "1"))
checkpoint_file = os.getenv("CHECKPOINT_FILE", f"checkpoint_{SHARD_INDEX}.txt")
last_i = int(open(checkpoint_file).read()) if os.path.exists(checkpoint_file) else 1

# ---------------- CHROME SETUP ---------------- #
chrome_options = Options()
chrome_options.add_argument("--headless=new")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--remote-debugging-port=9222")
chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")

# ---------------- GOOGLE SHEETS AUTH (Logic Fixed) ---------------- #
try:
    # Logic fix: Check if we are using an environment variable for credentials (common in CI/CD)
    creds_json = os.getenv("GSPREAD_CREDENTIALS")
    if creds_json:
        creds_dict = json.loads(creds_json)
        gc = gspread.service_account_from_dict(creds_dict)
    else:
        # Fallback to local file if not in CI/CD
        gc = gspread.service_account(filename="credentials.json")
    
    sheet_main = gc.open('Stock List').worksheet('Sheet1')
    sheet_data = gc.open('Tradingview Data Reel Experimental May').worksheet('Sheet5')
except Exception as e:
    print(f"🚨 Auth Error: {e}")
    exit(1)

# Batch read once
company_list = sheet_main.col_values(5)
name_list = sheet_main.col_values(1)
current_date = date.today().strftime("%m/%d/%Y")

# ---------------- CUSTOM EXPECTED CONDITION ---------------- #
class text_content_loaded:
    def __init__(self, selector, min_count=1):
        self.selector = selector
        self.min_count = min_count

    def __call__(self, driver):
        elements = driver.find_elements(By.CSS_SELECTOR, self.selector)
        non_empty_count = 0
        if len(elements) > 0:
            for el in elements:
                if el.text.strip():
                    non_empty_count += 1
            if non_empty_count >= self.min_count:
                return elements
        return False

# ---------------- SCRAPER (Selector Updated) ---------------- #
def scrape_tradingview(driver, company_url):
    # Selector updated for stability against TradingView CSS changes
    DATA_SELECTOR = "div[class^='valueValue-']"
    
    try:
        driver.get(company_url)
        WebDriverWait(driver, 60).until(text_content_loaded(DATA_SELECTOR, min_count=10))
        time.sleep(1)
        
        soup = BeautifulSoup(driver.page_source, "html.parser")
        val_elements = soup.find_all("div", class_=lambda x: x and 'valueValue-' in x)
        
        values = [
            el.get_text(strip=True).replace('−', '-').replace('∅', 'None')
            for el in val_elements if el.get_text(strip=True)
        ]
        return values
    except Exception as e:
        print(f"⚠️ Failed {company_url}: {e}")
        return []

# ---------------- MAIN LOOP (Logic Unchanged) ---------------- #
try:
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
except Exception as e:
    print(f"Error initializing WebDriver: {e}")
    exit(1)

if os.path.exists("cookies.json"):
    driver.get("https://www.tradingview.com/")
    with open("cookies.json", "r", encoding="utf-8") as f:
        cookies = json.load(f)
    for cookie in cookies:
        try:
            cookie_to_add = {k: cookie[k] for k in ('name', 'value', 'domain', 'path') if k in cookie}
            driver.add_cookie(cookie_to_add)
        except: pass
    driver.refresh()
    time.sleep(2)

buffer = []
BATCH_SIZE = 50

for i, company_url in enumerate(company_list[last_i:], last_i):
    if i % SHARD_STEP != SHARD_INDEX:
        continue
    if i > 2500: break

    name = name_list[i] if i < len(name_list) else f"Row {i}"
    print(f"Scraping {i}: {name}...", end=" ", flush=True)

    values = scrape_tradingview(driver, company_url)
    if values:
        buffer.append([name, current_date] + values)
        print(f"Done ({len(values)})")
    else:
        print("Skipped")

    with open(checkpoint_file, "w") as f:
        f.write(str(i))

    if len(buffer) >= BATCH_SIZE:
        try:
            sheet_data.append_rows(buffer, table_range='A1') 
            print(f"✅ Batch write success at index {i}")
            buffer.clear()
        except Exception as e:
            print(f"⚠️ Write failed: {e}")

    time.sleep(1.5 + random.random() * 1.5)

if buffer:
    try:
        sheet_data.append_rows(buffer, table_range='A1')
        print(f"✅ Final flush complete.")
    except Exception as e:
        print(f"⚠️ Final write failed: {e}")

driver.quit()
print("All done ✅")
