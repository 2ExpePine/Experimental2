from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
from bs4 import BeautifulSoup
import gspread
from datetime import date
import os
import time
import json
import random # <--- NEW: Added for randomized delay (jitter)
from webdriver_manager.chrome import ChromeDriverManager

# ---------------- SHARDING ---------------- #
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_STEP = int(os.getenv("SHARD_STEP", "1"))
checkpoint_file = os.getenv("CHECKPOINT_FILE", f"checkpoint_{SHARD_INDEX}.txt")
# Attempt to read last_i, defaulting to 1 if the file doesn't exist
last_i = int(open(checkpoint_file).read()) if os.path.exists(checkpoint_file) else 1

# ---------------- CHROME SETUP ---------------- #
chrome_options = Options()
chrome_options.add_argument("--headless=new")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--remote-debugging-port=9222")
# Recommended: Add a common user-agent to look less like a headless bot
chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")


# ---------------- GOOGLE SHEETS AUTH ---------------- #
try:
    # Use os.path.join for better path handling, though "credentials.json" is simple
    gc = gspread.service_account("credentials.json")
except Exception as e:
    print(f"Error loading credentials.json: {e}")
    exit(1)

sheet_main = gc.open('Stock List').worksheet('Sheet1')
sheet_data = gc.open('Tradingview Data Reel Experimental May').worksheet('Sheet5')

# Batch read once
company_list = sheet_main.col_values(5)
name_list = sheet_main.col_values(1)
current_date = date.today().strftime("%m/%d/%Y")

# ---------------- SCRAPER ---------------- #
# --- UPDATED: Increased timeout and changed locator for reliability ---
def scrape_tradingview(driver, company_url):
    try:
        driver.get(company_url)
        # 1. Increased timeout from 45s to 60s
        # 2. Changed locator to the more stable CSS selector for the main summary container
        WebDriverWait(driver, 60).until(
            EC.visibility_of_element_located((By.CSS_SELECTOR,
                '.container-qFvYvS1F')) # Reliable class for the main summary container
        )
        soup = BeautifulSoup(driver.page_source, "html.parser")
        values = [
            el.get_text().replace('−', '-').replace('∅', 'None')
            # The class below is for the data values that we want to extract
            for el in soup.find_all("div", class_="valueValue-l31H9iuA apply-common-tooltip")
        ]
        return values
    except NoSuchElementException:
        # It's useful to know when the page loaded but the target elements weren't found
        print(f"⚠️ Page loaded, but data elements not found on {company_url}. Returning empty.")
        return []
    except Exception as e:
        # General catch for timeouts, connection errors, etc.
        print(f"🚨 Timeout/Error scraping {company_url} after wait: {e}")
        return []

# ---------------- MAIN LOOP ---------------- #
# Initialize the driver once
try:
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
except Exception as e:
    print(f"Error initializing WebDriver: {e}")
    exit(1)


# Load cookies (once per shard)
if os.path.exists("cookies.json"):
    driver.get("https://www.tradingview.com/")
    with open("cookies.json", "r", encoding="utf-8") as f:
        cookies = json.load(f)
    for cookie in cookies:
        try:
            # Safely prepare cookie dictionary for Selenium
            cookie_to_add = {k: cookie[k] for k in ('name', 'value', 'domain', 'path') if k in cookie}
            cookie_to_add['secure'] = cookie.get('secure', False)
            cookie_to_add['httpOnly'] = cookie.get('httpOnly', False)
            # Add expiry if present and valid (Selenium needs int or float)
            if 'expiry' in cookie and cookie['expiry'] not in [None, '']:
                 cookie_to_add['expiry'] = int(cookie['expiry'])
            
            driver.add_cookie(cookie_to_add)
        except Exception:
            # Continue if a single cookie fails to load
            pass
    driver.refresh()
    time.sleep(2)
else:
    print("⚠️ cookies.json not found, scraping without login. Login may be required for full data access.")

buffer = []
BATCH_SIZE = 50

# Start loop from the last successful checkpoint
for i, company_url in enumerate(company_list[last_i:], last_i):
    # Sharding logic: skip if not this shard's turn
    if i % SHARD_STEP != SHARD_INDEX:
        continue
    
    # Safety limit to prevent runaway scripts and respect the list size
    if i >= 2235: # Use the known total count or 2500 as a hard stop
         print(f"Reached known list limit of 2235. Stopping.")
         break

    name = name_list[i] if i < len(name_list) else f"Row {i}"
    print(f"Scraping {i}: {name}")

    values = scrape_tradingview(driver, company_url)
    if values:
        buffer.append([name, current_date] + values)
    else:
        print(f"Skipping {name}: no data")

    # Write checkpoint
    with open(checkpoint_file, "w") as f:
        f.write(str(i))

    # Write every 50 rows
    if len(buffer) >= BATCH_SIZE:
        try:
            # Append rows starts appending from the first empty row
            sheet_data.append_rows(buffer, table_range='A1') 
            print(f"✅ Wrote batch of {len(buffer)} rows. Current row index: {i}")
            buffer.clear()
        except Exception as e:
            print(f"⚠️ Batch write failed: {e}. Data remaining in buffer.")

    # --- UPDATED: Increased sleep time and added jitter ---
    # Sleeps for a random duration between 1.5 and 3.0 seconds
    sleep_time = 1.5 + random.random() * 1.5 
    time.sleep(sleep_time)

# Final flush of any remaining items in the buffer
if buffer:
    try:
        sheet_data.append_rows(buffer, table_range='A1')
        print(f"✅ Final batch of {len(buffer)} rows written.")
    except Exception as e:
        print(f"⚠️ Final write failed: {e}")

driver.quit()
print("All done ✅")
