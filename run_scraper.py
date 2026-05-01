import sys
import os
import time
import json
import random
from datetime import date

# Use undetected_webdriver instead of standard selenium
import undetected_webdriver as uc 
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

# ---------------- BROWSER FACTORY (STEALTIER) ---------------- #
def create_driver():
    log("🌐 Initializing Stealth Hardened Chrome...")
    
    opts = uc.ChromeOptions()
    # Use headless=True for background, but note: non-headless is always harder to detect
    opts.add_argument("--headless") 
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    
    # Randomize User-Agent slightly if needed, or let UC handle it
    driver = uc.Chrome(options=opts, version_main=120) 
    
    driver.set_page_load_timeout(45)

    if os.path.exists("cookies.json"):
        try:
            driver.get("https://in.tradingview.com/")
            time.sleep(random.uniform(2, 4))
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
        
        # Human-like behavior: Small random scroll
        driver.execute_script("window.scrollTo(0, 300);")
        time.sleep(random.uniform(1, 3))
        
        # Wait for the specific container you mentioned
        WebDriverWait(driver, 45).until(
            EC.visibility_of_element_located((
                By.XPATH,
                '/html/body/div[2]/div/div[5]/div/div[1]/div/div[2]/div[1]/div[2]/div/div[1]/div[2]/div[2]/div[2]/div[2]/div'
            ))
        )
        
        soup = BeautifulSoup(driver.page_source, "html.parser")
        values = [
            el.get_text().replace('−', '-').replace('∅', 'None').strip()
            for el in soup.find_all(
                "div",
                class_="valueValue-l31H9iuA apply-common-tooltip"
            )
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
    log(f"✅ Setup complete | Shard {SHARD_INDEX} | Resume index {last_i}")
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
                # Longer sleep after a batch to avoid IP flags
                time.sleep(random.uniform(5, 10))
            except Exception as e:
                log(f"⚠️ API Error: {e}")
                if "429" in str(e):
                    time.sleep(60)

        with open(checkpoint_file, "w") as f:
            f.write(str(i + 1))

        # IMPORTANT: Randomize interaction time
        time.sleep(random.uniform(2, 5))

finally:
    if batch_list:
        try: sheet_data.batch_update(batch_list)
        except: pass
    driver.quit()
    log("🏁 Scraping completed successfully")
