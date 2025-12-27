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

# ---------------- SHARDING ---------------- #
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_STEP = int(os.getenv("SHARD_STEP", "1"))
checkpoint_file = os.getenv("CHECKPOINT_FILE", f"checkpoint_{SHARD_INDEX}.txt")
last_i = int(open(checkpoint_file).read()) if os.path.exists(checkpoint_file) else 1

print(f"🚀 STARTING SHARD {SHARD_INDEX}/{SHARD_STEP} from row {last_i}")

# ---------------- CHROME SETUP ---------------- #
chrome_options = Options()
chrome_options.add_argument("--headless=new")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--remote-debugging-port=9222")
chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")

# ---------------- GOOGLE SHEETS AUTH ---------------- #
print("📊 Connecting to Google Sheets...")
try:
    gc = gspread.service_account("credentials.json")
    print("✅ Credentials loaded")
except Exception as e:
    print(f"❌ Error loading credentials.json: {e}")
    exit(1)

try:
    sheet_main = gc.open('Stock List').worksheet('Sheet1')
    sheet_data = gc.open('Tradingview Data Reel Experimental May').worksheet('Sheet5')
    print("✅ Sheets connected: Stock List (Sheet1) → Tradingview Data Reel Experimental May (Sheet5)")
except Exception as e:
    print(f"❌ Sheet access error: {e}")
    exit(1)

# Batch read once
company_list = sheet_main.col_values(5)
name_list = sheet_main.col_values(1)
current_date = date.today().strftime("%m/%d/%Y")
print(f"📋 Loaded {len(company_list)} companies, {len(name_list)} names")

# ---------------- CUSTOM EXPECTED CONDITION ---------------- #
class text_content_loaded:
    """An expectation for checking that text content has loaded."""
    def __init__(self, locator, min_count=1):
        self.locator = locator
        self.min_count = min_count

    def __call__(self, driver):
        elements = driver.find_elements(*self.locator)
        non_empty_count = 0
        if len(elements) > 0:
            for el in elements:
                if el.text.strip():
                    non_empty_count += 1
            if non_empty_count >= self.min_count:
                return elements
        return False
        
# ---------------- SCRAPER ---------------- #
def scrape_tradingview(driver, company_url):
    DATA_LOCATOR = (By.CLASS_NAME, "valueValue-l31H9iuA") 
    
    try:
        print(f"   → Loading: {company_url}")
        driver.get(company_url)
        
        print(f"   ⏳ Waiting for data (75s timeout)...")
        WebDriverWait(driver, 75).until(
            text_content_loaded(DATA_LOCATOR, min_count=10) 
        )
        print(f"   ✅ Page loaded with data")
        
        soup = BeautifulSoup(driver.page_source, "html.parser")
        values = [
            el.get_text().replace('−', '-').replace('∅', 'None')
            for el in soup.find_all("div", class_="valueValue-l31H9iuA apply-common-tooltip")
        ]
        
        print(f"   📊 Found {len(values)} values")
        if len(values) > 0:
            print(f"   👀 Sample: {values[:3]}...")
            
        return values
        
    except (NoSuchElementException, TimeoutException):
        print(f"   ❌ TIMEOUT or NO ELEMENTS for {company_url}")
        return []
    except Exception as e:
        print(f"   🚨 ERROR scraping {company_url}: {e}")
        return []

# ---------------- MAIN LOOP ---------------- #
try:
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    print("✅ Chrome driver initialized")
except Exception as e:
    print(f"❌ Error initializing WebDriver: {e}")
    exit(1)

# Load cookies
cookies_loaded = False
if os.path.exists("cookies.json"):
    print("🍪 Loading cookies...")
    driver.get("https://www.tradingview.com/")
    try:
        with open("cookies.json", "r", encoding="utf-8") as f:
            cookies = json.load(f)
        for cookie in cookies:
            try:
                cookie_to_add = {k: cookie[k] for k in ('name', 'value', 'domain', 'path') if k in cookie}
                cookie_to_add['secure'] = cookie.get('secure', False)
                cookie_to_add['httpOnly'] = cookie.get('httpOnly', False)
                if 'expiry' in cookie and cookie['expiry'] not in [None, '']:
                     cookie_to_add['expiry'] = int(cookie['expiry'])
                driver.add_cookie(cookie_to_add)
            except Exception:
                pass
        driver.refresh()
        time.sleep(2)
        print("✅ Cookies loaded")
        cookies_loaded = True
    except Exception as e:
        print(f"⚠️ Cookie loading failed: {e}")
else:
    print("⚠️ cookies.json not found - running without login")

buffer = []
BATCH_SIZE = 50
total_rows_written = 0
successful_scrapes = 0
total_processed = 0

print(f"🔄 Starting main loop from {last_i}...")

# Start loop from the last successful checkpoint
for i, company_url in enumerate(company_list[last_i:], last_i):
    if i % SHARD_STEP != SHARD_INDEX:
        continue
    
    if i > 2500: 
        print("🛑 Reached scraping limit of 2500. Stopping.")
        break

    total_processed += 1
    name = name_list[i] if i < len(name_list) else f"Row {i}"
    print(f"\n[{total_processed}] Scraping {i}: {name}")

    values = scrape_tradingview(driver, company_url)
    
    if values:
        buffer.append([name, current_date] + values)
        successful_scrapes += 1
        print(f"   ✅ Added to buffer ({len(buffer)}/{BATCH_SIZE})")
    else:
        print(f"   ❌ No data - skipped")

    # Write checkpoint
    with open(checkpoint_file, "w") as f:
        f.write(str(i))
    
    print(f"   💾 Checkpoint: {i}")

    # Write every 50 rows
    if len(buffer) >= BATCH_SIZE:
        try:
            sheet_data.append_rows(buffer)
            total_rows_written += len(buffer)
            print(f"✅ BATCH WRITTEN: {len(buffer)} rows | TOTAL: {total_rows_written} | Current: {i}")
            buffer.clear()
        except Exception as e:
            print(f"❌ BATCH WRITE FAILED: {e}")
            print(f"   Buffer size: {len(buffer)}")

    # Sleep with jitter
    sleep_time = 1.5 + random.random() * 1.5 
    print(f"   😴 Sleeping {sleep_time:.1f}s...")
    time.sleep(sleep_time)

# Final flush
if buffer:
    try:
        sheet_data.append_rows(buffer)
        total_rows_written += len(buffer)
        print(f"✅ FINAL BATCH: {len(buffer)} rows | GRAND TOTAL: {total_rows_written}")
    except Exception as e:
        print(f"❌ FINAL WRITE FAILED: {e}")

print(f"\n🎉 FINISHED!")
print(f"📈 Total processed: {total_processed}")
print(f"✅ Successful scrapes: {successful_scrapes}")
print(f"💾 Rows written to sheet: {total_rows_written}")
print(f"📊 Buffer leftovers: {len(buffer)}")

driver.quit()
print("👋 Driver closed")
