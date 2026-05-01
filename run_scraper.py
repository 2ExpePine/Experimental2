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
    
    def get_options():
        opt = uc.ChromeOptions()
        opt.add_argument("--headless")
        opt.add_argument("--no-sandbox")
        opt.add_argument("--disable-dev-shm-usage")
        opt.add_argument("--disable-gpu")
        opt.add_argument("--window-size=1920,1080")
        return opt

    try:
        # Pinned to 147 as per your environment logs
        driver = uc.Chrome(options=get_options(), use_subprocess=True, version_main=147) 
    except Exception as e:
        log(f"⚠️ Launch failed, trying fallback: {str(e)[:50]}")
        driver = uc.Chrome(options=get_options(), version_main=147)

    driver.set_page_load_timeout(90)

    if os.path.exists("cookies.json"):
        try:
            driver.get("https://in.tradingview.com/")
            time.sleep(5)
            with open("cookies.json", "r") as f:
                cookies = json.load(f)
            for c in cookies:
                try:
                    driver.add_cookie({k: v for k, v in c.items() if k in ("name", "value", "path", "secure", "expiry")})
                except: continue
            driver.refresh()
            time.sleep(3)
            log("✅ Cookies applied")
        except: pass

    return driver

# ---------------- SCRAPER LOGIC ---------------- #
def scrape_tradingview(driver, url):
    try:
        driver.get(url)
        # Give extra time for the technical section to render
        time.sleep(random.uniform(8, 12)) 
        
        # Scroll down slightly to trigger lazy-loaded elements
        driver.execute_script("window.scrollTo(0, 400);")
        time.sleep(2)

        # Try multiple XPaths in case the layout shifts
        potential_xpaths = [
            '/html/body/div[2]/div/div[5]/div/div[1]/div/div[2]/div[1]/div[2]/div/div[1]/div[2]/div[2]/div[2]/div[2]/div',
            '//div[contains(@class, "valueValue-l31H9iuA")]'
        ]
        
        found = False
        for path in potential_xpaths:
            try:
                WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.XPATH, path)))
                found = True
                break
            except: continue

        if not found:
            return []

        soup = BeautifulSoup(driver.page_source, "html.parser")
        # Look for the specific value class you need
        raw_values = soup.find_all("div", class_=lambda x: x and "valueValue-" in x)
        
        values = [
            v.get_text().replace('−', '-').replace('∅', 'None').strip()
            for v in raw_values
        ]
        
        return values
        
    except Exception as e:
        log(f"🛑 Error during scrape: {str(e)[:50]}")
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
BATCH_SIZE = 5 # Reduced batch size for safer saving

try:
    for i in range(last_i, len(company_list)):
        if i % SHARD_STEP != SHARD_INDEX:
            continue
        if i >= 2500: break

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

        if isinstance(values, list) and len(values) > 0:
            target_row = i + 1
            batch_list.append({
                "range": f"A{target_row}",
                "values": [[name, current_date] + values]
            })
            log(f"📦 Buffered ({len(batch_list)}/{BATCH_SIZE})")
        else:
            log(f"⏭️ Skipped {name} (No data found)")

        if len(batch_list) >= BATCH_SIZE:
            try:
                sheet_data.batch_update(batch_list)
                log(f"🚀 Saved batch to Google Sheets")
                batch_list = []
                time.sleep(random.uniform(5, 8)) 
            except Exception as e:
                log(f"⚠️ Sheets Error: {e}")
                time.sleep(30)

        with open(checkpoint_file, "w") as f:
            f.write(str(i + 1))

finally:
    if batch_list:
        try: sheet_data.batch_update(batch_list)
        except: pass
    if 'driver' in locals():
        driver.quit()
    log("🏁 Scraping session ended.")
