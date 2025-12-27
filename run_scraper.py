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

print("🚀 TradingView Scraper v2 - Fixed selectors")

# ---------------- SHARDING & SETUP (same as before) ---------------- #
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_STEP = int(os.getenv("SHARD_STEP", "1"))
checkpoint_file = os.getenv("CHECKPOINT_FILE", f"checkpoint_{SHARD_INDEX}.txt")
last_i = int(open(checkpoint_file).read().strip()) if os.path.exists(checkpoint_file) else 1

chrome_options = Options()
chrome_options.add_argument("--headless=new")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")

# ---------------- SHEETS ---------------- #
gc = gspread.service_account("credentials.json")
sheet_main = gc.open('Stock List').worksheet('Sheet1')
sheet_data = gc.open('Tradingview Data Reel Experimental May').worksheet('Sheet5')

company_list = sheet_main.col_values(5)
name_list = sheet_main.col_values(1)
current_date = date.today().strftime("%m/%d/%Y")

print(f"✅ Loaded {len(company_list)} companies from row {last_i}")

# ---------------- NEW ROBUST SCRAPER ---------------- #
def scrape_tradingview(driver, company_url, name):
    print(f"   → {name[:30]}...")
    
    try:
        driver.get(company_url)
        time.sleep(10)  # Let page load
        
        # DEBUG: Save page source to see what's actually there
        with open("debug_page.html", "w", encoding="utf-8") as f:
            f.write(driver.page_source)
        print("   📄 Page source saved to debug_page.html")
        
        soup = BeautifulSoup(driver.page_source, "html.parser")
        
        # STRATEGY 1: Find numbers in data-value attributes
        values = []
        data_values = soup.find_all(attrs={"data-value": True})
        for el in data_values[:50]:
            val = el.get("data-value", "").strip()
            if val and val not in ["None", "", "0"]:
                values.append(val.replace('−', '-'))
        
        # STRATEGY 2: Find all numeric text in common TradingView containers
        if not values:
            tv_containers = soup.find_all(["div", "span"], class_=re.compile(r"(value|data|metric|price|tv-.*)"))
            for el in tv_containers[:100]:
                text = el.get_text().strip()
                # Match numbers, percentages, etc.
                if re.match(r'[-+]?\d+(?:\.\d+)?(?:[TBMK]?|\s?%)?', text):
                    values.append(text.replace('−', '-'))
        
        # STRATEGY 3: Find elements with numeric classes/content
        if not values:
            numeric_elements = soup.find_all(string=re.compile(r'[-+]?\d+(?:\.\d+)?'))
            values = list(set([str(v).strip() for v in numeric_elements[:50] if v.strip()]))
        
        # STRATEGY 4: Direct Selenium text extraction from common data areas
        if not values:
            try:
                # Look for common TradingView data holders
                locators = [
                    (By.CSS_SELECTOR, "[class*='value']"),
                    (By.CSS_SELECTOR, "[class*='data']"), 
                    (By.CSS_SELECTOR, "[class*='price']"),
                    (By.CSS_SELECTOR, "[class*='metric']")
                ]
                for by, selector in locators:
                    elements = driver.find_elements(by, selector)
                    values = [el.text.strip() for el in elements[:50] if el.text.strip() and re.search(r'\d', el.text)]
                    if values:
                        break
            except:
                pass
        
        print(f"   📊 Found {len(values)} values: {values[:5] if values else 'EMPTY'}")
        return values[:30]  # Limit columns
        
    except Exception as e:
        print(f"   ❌ Error: {str(e)[:80]}")
        return []

# ---------------- MAIN ---------------- #
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

buffer = []
BATCH_SIZE = 5  # Tiny batches for testing
total_written = 0

for i, company_url in enumerate(company_list[last_i:], last_i):
    if i % SHARD_STEP != SHARD_INDEX:
        continue
    if i > 10:  # Test first 10 only
        break
        
    name = name_list[i] if i < len(name_list) else f"Row {i}"
    print(f"\n[{i}] {name}")
    
    values = scrape_tradingview(driver, company_url, name)
    
    if values:
        row = [name, current_date] + values
        buffer.append(row)
        print(f"   ✅ Row ready: {len(row)} cells")
        
        # Checkpoint
        with open(checkpoint_file, "w") as f:
            f.write(str(i))
        
        # Write immediately for testing
        if len(buffer) >= BATCH_SIZE:
            try:
                sheet_data.append_rows(buffer)
                total_written += len(buffer)
                print(f"✅ WRITTEN {len(buffer)} rows | TOTAL: {total_written}")
                buffer = []
            except Exception as e:
                print(f"❌ WRITE ERROR: {e}")
    else:
        print("   ❌ NO DATA - skipping")
    
    time.sleep(3)

# Final write
if buffer:
    sheet_data.append_rows(buffer)
    print(f"✅ FINAL {len(buffer)} rows")

driver.quit()
print(f"\n🎉 DONE: {total_written} rows written")
