import sys
import os
import time
import json
from datetime import date
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
import gspread
from webdriver_manager.chrome import ChromeDriverManager

# ---------------- CONFIG ---------------- #
EXPECTED_COUNT = 27
RESTART_EVERY = 20

def log(msg):
    print(msg, flush=True)

# ---------------- SHARD ---------------- #
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_STEP = int(os.getenv("SHARD_STEP", "1"))

checkpoint_file = os.getenv("CHECKPOINT_FILE", f"checkpoint_{SHARD_INDEX}.txt")
last_i = int(open(checkpoint_file).read()) if os.path.exists(checkpoint_file) else 0

# ---------------- DRIVER ---------------- #
def create_driver():
    log("🌐 Initializing Chrome...")
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-blink-features=AutomationControlled")

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=opts
    )
    driver.set_page_load_timeout(40)

    # Cookies
    if os.path.exists("cookies.json"):
        try:
            driver.get("https://in.tradingview.com/")
            time.sleep(2)
            with open("cookies.json", "r") as f:
                cookies = json.load(f)
            for c in cookies:
                try:
                    driver.add_cookie({k: v for k, v in c.items() if k in ("name", "value", "path", "secure", "expiry")})
                except:
                    pass
            driver.refresh()
            log("✅ Cookies applied")
        except:
            pass

    return driver

# ---------------- SCRAPER ---------------- #
def get_values(driver):
    elements = driver.find_elements(By.CSS_SELECTOR, "[class*='valueValue']")
    return [el.text.strip().replace('−', '-').replace('∅', 'None') for el in elements if el.text.strip()]

def scrape_tradingview(driver, url):
    for attempt in range(2):
        try:
            driver.get(url)

            # wait until enough elements appear
            WebDriverWait(driver, 30).until(
                lambda d: len(d.find_elements(By.CSS_SELECTOR, "[class*='valueValue']")) > 10
            )

            time.sleep(2)

            values = get_values(driver)

            # scroll if incomplete
            if len(values) < EXPECTED_COUNT:
                for y in [800, 1500, 2500]:
                    driver.execute_script(f"window.scrollTo(0, {y});")
                    time.sleep(1.5)
                    values = get_values(driver)
                    if len(values) >= EXPECTED_COUNT:
                        break

            # stability check
            time.sleep(1.5)
            values2 = get_values(driver)

            if values != values2:
                log("⚠️ Data unstable → retry")
                continue

            if len(values) >= EXPECTED_COUNT:
                log(f"✅ Got {len(values)}/{EXPECTED_COUNT}")
                return values[:EXPECTED_COUNT]

            log(f"⚠️ Only {len(values)} values → retry")

        except Exception as e:
            log(f"❌ Attempt {attempt+1} failed: {str(e)[:60]}")

    return []

# ---------------- SHEETS ---------------- #
log("📊 Connecting to Google Sheets...")
try:
    gc = gspread.service_account("credentials.json")
    sheet_main = gc.open("Stock List").worksheet("Sheet1")
    sheet_data = gc.open("Tradingview Data Reel Experimental May").worksheet("Sheet5")

    company_list = sheet_main.col_values(5)
    name_list = sheet_main.col_values(1)

    current_date = date.today().strftime("%m/%d/%Y")
    log(f"✅ Setup done | Resume {last_i}")
except Exception as e:
    log(f"❌ Setup Error: {e}")
    sys.exit(1)

# ---------------- MAIN LOOP ---------------- #
driver = create_driver()
batch_list = []
BATCH_SIZE = 100

try:
    for i in range(last_i, len(company_list)):

        if i % SHARD_STEP != SHARD_INDEX:
            continue
        if i >= 2500:
            break

        # periodic restart
        if i % RESTART_EVERY == 0 and i != 0:
            log("🔄 Restarting browser...")
            try:
                driver.quit()
            except:
                pass
            driver = create_driver()

        url = company_list[i]
        name = name_list[i] if i < len(name_list) else f"Row {i}"

        log(f"🔍 [{i}] {name}")

        values = scrape_tradingview(driver, url)

        # restart if crash
        if values == "RESTART":
            driver.quit()
            driver = create_driver()
            values = scrape_tradingview(driver, url)

        # STRICT validation
        if isinstance(values, list) and len(values) >= EXPECTED_COUNT:
            target_row = i + 1
            batch_list.append({
                "range": f"A{target_row}",
                "values": [[name, current_date] + values]
            })
            log(f"📦 Buffered ({len(batch_list)}/{BATCH_SIZE})")
        else:
            log(f"⏭️ Skipped (bad data)")

        # batch upload
        if len(batch_list) >= BATCH_SIZE:
            try:
                sheet_data.batch_update(batch_list)
                log(f"🚀 Saved {len(batch_list)} rows")
                batch_list = []
            except Exception as e:
                log(f"⚠️ API Error: {e}")
                if "429" in str(e):
                    log("⏳ Sleeping 60s...")
                    time.sleep(60)

        # checkpoint
        with open(checkpoint_file, "w") as f:
            f.write(str(i + 1))

        time.sleep(0.5)

finally:
    if batch_list:
        try:
            sheet_data.batch_update(batch_list)
            log(f"✅ Final save: {len(batch_list)} rows")
        except:
            pass

    driver.quit()
    log("🏁 COMPLETED")
