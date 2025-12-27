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
import re

print("🚀 STARTING - FIXED VERSION")

# ---------------- FIXED SHARDING ---------------- #
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_STEP = int(os.getenv("SHARD_STEP", "1"))
checkpoint_file = os.getenv("CHECKPOINT_FILE", f"checkpoint_{SHARD_INDEX}.txt")

# FIXED CHECKPOINT READ
try:
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, 'r') as f:
            last_i = int(f.read().strip())
        print(f"✅ Checkpoint loaded: row {last_i}")
    else:
        last_i = 0
        print("📋 Starting from row 0 (no checkpoint)")
except:
    last_i = 0
    print("🔄 Checkpoint corrupt, starting from 0")

# ---------------- CHROME & SHEETS ---------------- #
chrome_options = Options()
chrome_options.add_argument("--headless=new")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")

gc = gspread.service_account("credentials.json")
sheet_main = gc.open('Stock List').worksheet('Sheet1')
sheet_data = gc.open('Tradingview Data Reel Experimental May').worksheet('Sheet5')

company_list = sheet_main.col_values(5)
name_list = sheet_main.col_values(1)
current_date = date.today().strftime("%m/%d/%Y")

print(f"📊 Loaded {len(company_list)} URLs from Column E, starting at row {last_i}")

# ---------------- FIXED SCRAPER ---------------- #
def scrape_tradingview(driver, company_url, name, i):
    print(f"[{i}] → {name[:30]}...")
    
    try:
        driver.get(company_url)
        time.sleep(8)  # Fixed wait
        
        # FIXED: Multiple selectors + direct Selenium
        soup = BeautifulSoup(driver.page_source, "html.parser")
        
        # Strategy 1: Any numeric data-value attributes
        values = []
        for el in soup.find_all(attrs={"data-value": True})[:30]:
            val = el.get("data-value", "").strip()
            if re.match(r'[-+]?\d+(?:\.\d+)?', val):
                values.append(val.replace('−', '-'))
        
        # Strategy 2: Common TradingView number patterns
        if not values:
            for el in soup.find_all(text=re.compile(r'[-+]?\d+(?:\.\d+)?'))[:30]:
                val = str(el).strip()
                if len(val) < 20 and val not in ['0', '1', 'None']:
                    values.append(val.replace('−', '-'))
        
        # Strategy 3: Selenium direct grab
        if not values:
            elements = driver.find_elements(By.CSS_SELECTOR, "[class*='value'], [class*='price'], [class*='data']")
            values = [el.text.strip() for el in elements[:30] if re.search(r'\d', el.text) and len(el.text.strip()) < 15]
        
        print(f"   📊 {len(values)} values: {values[:3] if values else 'NONE'}")
        return values
        
    except Exception as e:
        print(f"   ❌ ERROR: {str(e)[:50]}")
        return []

# ---------------- MAIN LOOP ---------------- #
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
print("✅ Chrome ready")

buffer = []
BATCH_SIZE = 10  # Smaller for testing
total_written = 0
processed = 0

for i, company_url in enumerate(company_list[last_i:], last_i):
    if i % SHARD_STEP != SHARD_INDEX:
        continue
    if i > 20:  # Test first 20 only
        print("🛑 Test limit reached")
        break
    
    processed += 1
    name = name_list[i] if i < len(name_list) else f"Row {i}"
    
    values = scrape_tradingview(driver, company_url, name, i)
    
    if values:
        buffer.append([name, current_date] + values)
        print(f"   ✅ BUFFER: {len(buffer)}/{BATCH_SIZE}")
        
        # Checkpoint EVERY row
        try:
            with open(checkpoint_file, "w") as f:
                f.write(str(i))
        except:
            pass
        
        # Write small batches
        if len(buffer) >= BATCH_SIZE:
            try:
                sheet_data.append_rows(buffer)
                total_written += len(buffer)
                print(f"✅ WRITTEN {len(buffer)} rows | TOTAL: {total_written}")
                buffer = []
            except Exception as e:
                print(f"❌ WRITE FAILED: {e}")
    else:
        print(f"   ❌ NO DATA SKIPPED")
    
    time.sleep(2)

# Final write
if buffer:
    try:
        sheet_data.append_rows(buffer)
        total_written += len(buffer)
        print(f"✅ FINAL {len(buffer)} rows")
    except Exception as e:
        print(f"❌ FINAL WRITE FAILED: {e}")

print(f"\n🎉 SUMMARY:")
print(f"   Processed: {processed}")
print(f"   Rows written: {total_written}")
print(f"   Checkpoint: {i}")

driver.quit()
