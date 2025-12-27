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
chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")

# ---------------- GOOGLE SHEETS AUTH ---------------- #
try:
    gc = gspread.service_account("credentials.json")
    sheet_main = gc.open_by_key("1V8DsH-R3vdUbXqDKZYWHk_8T0VRjqTEVyj7PhlIDtG4").worksheet("Sheet1")
    sheet_data = gc.open('Tradingview Data Reel Experimental May').worksheet('Sheet5')
except Exception as e:
    print(f"Error loading credentials or opening sheets: {e}")
    exit(1)

company_list = sheet_main.col_values(5)
name_list = sheet_main.col_values(1)
current_date = date.today().strftime("%m/%d/%Y")

# ---------------- SCRAPER ---------------- #
def scrape_tradingview(driver, company_url):
    # UPDATED: Flexible selector to handle TradingView's dynamic class suffixes
    DATA_SELECTOR = "div[class^='valueValue-']" 
    
    try:
        driver.get(company_url)
        
        # Wait for the elements to be present in the DOM
        WebDriverWait(driver, 45).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, DATA_SELECTOR))
        )
        
        # Short sleep to allow React to finish populating text content
        time.sleep(2)
        
        soup = BeautifulSoup(driver.page_source, "html.parser")
        
        # Find all divs where class starts with 'valueValue-'
        elements = soup.find_all("div", class_=lambda x: x and x.startswith("valueValue-"))
        
        values = [
            el.get_text(strip=True).replace('−', '-').replace('∅', 'None')
            for el in elements if el.get_text(strip=True)
        ]
        
        return values
        
    except (NoSuchElementException, TimeoutException):
        print(f"⚠️ Timeout/Not Found for {company_url}")
        return []
    except Exception as e:
        print(f"🚨 Error: {e}")
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
        except:
            pass
    driver.refresh()
    time.sleep(2)

buffer = []
BATCH_SIZE = 20 # Lowered slightly so you see data in the sheet sooner

for i, company_url in enumerate(company_list[last_i:], last_i):
    if i % SHARD_STEP != SHARD_INDEX:
        continue
    
    if i > 2500: 
        break

    name = name_list[i] if i < len(name_list) else f"Row {i}"
    print(f"Scraping {i}: {name}")

    values = scrape_tradingview(driver, company_url)
    if values:
        buffer.append([name, current_date] + values)
    else:
        print(f"Skipping {name}: no data")

    # Checkpoint logic
    with open(checkpoint_file, "w") as f:
        f.write(str(i))

    # Batch write logic
    if len(buffer) >= BATCH_SIZE:
        try:
            sheet_data.append_rows(buffer) 
            print(f"✅ Wrote batch of {len(buffer)} rows.")
            buffer.clear()
        except Exception as e:
            print(f"⚠️ Batch write failed: {e}")

    time.sleep(1.5 + random.random() * 1.5)

# Final flush
if buffer:
    try:
        sheet_data.append_rows(buffer)
        print(f"✅ Final batch written.")
    except Exception as e:
        print(f"⚠️ Final write failed: {e}")

driver.quit()
print("All done ✅")
