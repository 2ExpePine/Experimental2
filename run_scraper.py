import sys
import os
import time
import json
import random
from datetime import date

import undetected_chromedriver as uc 
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException, WebDriverException

from bs4 import BeautifulSoup
import gspread

def log(msg):
    print(msg, flush=True)

# ---------------- CONFIG & SHARDING ---------------- #
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_STEP = int(os.getenv("SHARD_STEP", "1"))

checkpoint_file = os.getenv("CHECKPOINT_FILE", f"checkpoint_{SHARD_INDEX}.txt")
last_i = int(open(checkpoint_file).read()) if os.path.exists(checkpoint_file) else 0

# ---------------- BROWSER FACTORY ---------------- #
def create_driver():
    log("🌐 Initializing Stealth Chrome for GitHub Actions...")
    
    options = uc.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    
    # Bypass simple detection
    driver = uc.Chrome(options=options) 
    driver.set_page_load_timeout(60)

    if os.path.exists("cookies.json"):
        try:
            driver.get("https://in.tradingview.com/")
            time.sleep(random.uniform(3, 5))
            with open("cookies.json", "r") as f:
                cookies = json.load(f)
            for c in cookies:
                try:
                    driver.add_cookie({
                        k: v for k, v in c.items()
                        if k in ("name", "value", "path", "secure", "expiry")
                    })
                except: continue
            driver.refresh()
            time.sleep(2)
            log("✅ Cookies applied")
        except Exception as e:
            log(f"⚠️ Cookie error: {str(e)[:60]}")

    return driver

# ---------------- SCRAPER LOGIC ---------------- #
def scrape_tradingview(driver, url):
    try:
        driver.get(url)
        time.sleep(random.uniform(2, 4)) # Wait for initial load
        
        # Hardcoded specific container wait
        target_xpath = '/html/body/div[2]/div/div[5]/div/div[1]/div/div[2]/div[1]/div[2]/div/div[1]/div[2]/div[2]/div[2]/div[2]/div'
        
        WebDriverWait(driver, 45).until(
            EC.visibility_of_element_located((By.XPATH, target_xpath))
        )
        
        soup = BeautifulSoup(driver.page_source, "html.parser")
        values = [
            el.get_text().replace('−', '-').replace('∅', 'None').strip()
            for el in soup.find_all("div", class_="valueValue-l31H9iuA apply-common-tooltip")
        ]
        return values
        
    except (TimeoutException, NoSuchElementException):
        return []
    except WebDriverException:
        log("🛑 Browser Crash Detected")
        return "RESTART"

# ---------------- INITIAL SETUP ---------------- #
log("📊 Connecting to Google Sheets...")
try:
    gc = gspread.service_account("credentials.json")
    sheet_main = gc.open("Stock List").worksheet("Sheet1")
    sheet_data = gc.open("Tradingview Data Reel Experimental May").worksheet("Sheet5")

    company_list = sheet_main.col_values(5)
    name_list = sheet_main.col_values(1)

    current_date = date.today().strftime("%m/%d/%Y")
    log(f"✅ Setup complete | Shard {SHARD_INDEX} | Start index {last_i}")
except Exception as e:
    log(f"❌ Setup Error: {e}")
    sys.exit(1)

# ---------------- MAIN LOOP ---------------- #
driver = create_driver()
batch_list = []
BATCH_SIZE = 10

try:
    for i in range(last_i, len(company_list)):
        if i % SHARD_STEP != SHARD_INDEX:
            continue
        if i >= 2500:
            break

        url = company_list[i]
        name = name_list[i] if i < len(name_list) else f"Row {i}"

        log(f"🔍 [{i}] Scraping: {name}")

        values = scrape_tradingview(driver, url)

        if values == "RESTART":
            try: driver.quit()
            except: pass
            driver = create_driver()
            values = scrape_tradingview(driver, url)
            if values == "RESTART": values = []

        if isinstance(values, list) and values:
            target_row = i + 1
            batch_list.append({
                "range": f"A{target_row}",
                "values": [[name, current_date] + values]
            })
            log(f"📦 Buffered ({len(batch_list)}/{BATCH_SIZE})")
        else:
            log(f"⏭️ Skipped {name}")

        if len(batch_list) >= BATCH_SIZE:
            try:
                sheet_data.batch_update(batch_list)
                log(f"🚀 Saved {len(batch_list)} rows")
                batch_list = []
                time.sleep(random.uniform(5, 10)) # Anti-block pause
            except Exception as e:
                log(f"⚠️ API Error: {e}")
                if "429" in str(e):
                    time.sleep(60)

        with open(checkpoint_file, "w") as f:
            f.write(str(i + 1))

        time.sleep(random.uniform(2, 5)) # Random delay between stocks

finally:
    if batch_list:
        try: sheet_data.batch_update(batch_list)
        except: pass
    driver.quit()
    log("🏁 Scraping completed successfully")
