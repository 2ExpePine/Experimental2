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

print("🚀 Script starting...")

# ---------------- SHARDING ---------------- #
try:
    SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
    SHARD_STEP = int(os.getenv("SHARD_STEP", "1"))
    checkpoint_file = os.getenv("CHECKPOINT_FILE", f"checkpoint_{SHARD_INDEX}.txt")
    last_i = int(open(checkpoint_file).read().strip()) if os.path.exists(checkpoint_file) else 1
    print(f"✅ Shard {SHARD_INDEX}/{SHARD_STEP} starting from row {last_i}")
except Exception as e:
    print(f"❌ Shard setup failed: {e}")
    last_i = 1
    SHARD_INDEX = 0
    SHARD_STEP = 1

# ---------------- CHROME SETUP ---------------- #
chrome_options = Options()
chrome_options.add_argument("--headless=new")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--remote-debugging-port=9222")
chrome_options.add_argument("--disable-web-security")
chrome_options.add_argument("--disable-features=VizDisplayCompositor")
chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")

print("🔧 Chrome options set")

# ---------------- GOOGLE SHEETS AUTH ---------------- #
print("📊 Loading Google Sheets credentials...")
try:
    gc = gspread.service_account("credentials.json")
    print("✅ Credentials loaded")
except FileNotFoundError:
    print("❌ credentials.json NOT FOUND - this will fail!")
    exit(1)
except Exception as e:
    print(f"❌ Credentials error: {e}")
    exit(1)

try:
    sheet_main = gc.open('Stock List').worksheet('Sheet1')
    sheet_data = gc.open('Tradingview Data Reel Experimental May').worksheet('Sheet5')
    print("✅ Sheets connected")
except Exception as e:
    print(f"❌ Sheet connection failed: {e}")
    exit(1)

# Batch read once
print("📋 Reading company data...")
try:
    company_list = sheet_main.col_values(5)
    name_list = sheet_main.col_values(1)
    current_date = date.today().strftime("%m/%d/%Y")
    print(f"✅ Loaded {len(company_list)} companies, {len(name_list)} names")
except Exception as e:
    print(f"❌ Sheet read failed: {e}")
    exit(1)

# ---------------- CUSTOM EXPECTED CONDITION ---------------- #
class text_content_loaded:
    def __init__(self, locator, min_count=1):
        self.locator = locator
        self.min_count = min_count

    def __call__(self, driver):
        elements = driver.find_elements(*self.locator)
        non_empty_count = sum(1 for el in elements if el.text.strip())
        return elements if non_empty_count >= self.min_count else False

# ---------------- SCRAPER ---------------- #
def scrape_tradingview(driver, company_url, name):
    print(f"   → Scraping: {name[:30]}...")
    DATA_LOCATOR = (By.CLASS_NAME, "valueValue-l31H9iuA")
    
    try:
        driver.get(company_url)
        print(f"   ⏳ Waiting 75s for data...")
        
        WebDriverWait(driver, 75).until(
            text_content_loaded(DATA_LOCATOR, min_count=5)  # Reduced to 5 for GitHub timeout
        )
        print(f"   ✅ Data loaded")
        
        # Try multiple selectors for TradingView data
        soup = BeautifulSoup(driver.page_source, "html.parser")
        values = []
        
        # Original selector
        values = [el.get_text().replace('−', '-').replace('∅', 'None').strip()
                 for el in soup.find_all("div", class_="valueValue-l31H9iuA apply-common-tooltip")]
        
        # Fallback selectors if empty
        if not values:
            values = [el.get_text().strip() for el in soup.find_all("div", class_="valueValue-l31H9iuA")]
        if not values:
            values = [el.text.strip() for el in driver.find_elements(*DATA_LOCATOR) if el.text.strip()]
            
        print(f"   📊 Found {len(values)} values")
        return values[:50]  # Limit for sheet
        
    except Exception as e:
        print(f"   ❌ Scrape failed: {str(e)[:100]}")
        return []

# ---------------- MAIN EXECUTION ---------------- #
print("🚗 Initializing Chrome...")
try:
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    print("✅ Chrome ready")
except Exception as e:
    print(f"❌ Chrome failed: {e}")
    exit(1)

# Load cookies if exist
if os.path.exists("cookies.json"):
    try:
        driver.get("https://www.tradingview.com/")
        with open("cookies.json", "r") as f:
            cookies = json.load(f)
        for cookie in cookies[:20]:  # Limit cookies
            try:
                driver.add_cookie(cookie)
            except:
                pass
        driver.refresh()
        print("✅ Cookies loaded")
        time.sleep(3)
    except Exception as e:
        print(f"⚠️ Cookies failed: {e}")

buffer = []
BATCH_SIZE = 10  # Smaller for GitHub Actions
total_written = 0
successful_scrapes = 0

print(f"🔄 Processing from row {last_i}...")

for i, company_url in enumerate(company_list[last_i:], last_i):
    if i % SHARD_STEP != SHARD_INDEX:
        continue
    if i > 2500:
        print("🛑 Limit reached")
        break

    name = name_list[i] if i < len(name_list) else f"Row {i}"
    print(f"\n[{i}] {name[:40]}")

    values = scrape_tradingview(driver, company_url, name)
    
    if values:
        buffer.append([name, current_date] + values)
        successful_scrapes += 1
        print(f"   ✅ Added ({len(values)} values)")
        
        # Checkpoint every row for GitHub
        try:
            with open(checkpoint_file, "w") as f:
                f.write(str(i))
        except:
            pass

        # Smaller batches for GitHub Actions
        if len(buffer) >= BATCH_SIZE:
            try:
                sheet_data.append_rows(buffer)
                total_written += len(buffer)
                print(f"✅ BATCH: {len(buffer)} rows | TOTAL: {total_written}")
                buffer = []
            except Exception as e:
                print(f"❌ Write failed: {e}")

    # Rate limit
    time.sleep(2 + random.random() * 2)

# Final write
if buffer:
    try:
        sheet_data.append_rows(buffer)
        total_written += len(buffer)
        print(f"✅ FINAL: {len(buffer)} | TOTAL: {total_written}")
    except Exception as e:
        print(f"❌ Final write failed: {e}")

print(f"\n🎉 SUMMARY:")
print(f"   Successful scrapes: {successful_scrapes}")
print(f"   Rows written: {total_written}")
print(f"   Final checkpoint: {last_i if not company_list[last_i:] else i}")

driver.quit()
print("✅ COMPLETE")
