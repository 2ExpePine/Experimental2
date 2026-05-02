import sys
import os
import time
import json
import random
from datetime import date
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import gspread
from webdriver_manager.chrome import ChromeDriverManager

# ---------------- LOG ---------------- #
def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

# ---------------- CONFIG ---------------- #
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_SIZE = int(os.getenv("SHARD_SIZE", "500"))
START_ROW = SHARD_INDEX * SHARD_SIZE
END_ROW = START_ROW + SHARD_SIZE

EXPECTED_COUNT = 7
BATCH_SIZE = 50
RESTART_EVERY_ROWS = 10   # more frequent restart

CHROME_DRIVER_PATH = ChromeDriverManager().install()

# ---------------- DRIVER ---------------- #
driver = None

def create_driver():
    log("🌐 Initializing browser (persistent session)...")

    opts = Options()

    # ❌ IMPORTANT: NO HEADLESS
    # opts.add_argument("--headless=new")

    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")

    # ✅ persistent profile (VERY IMPORTANT)
    opts.add_argument("--user-data-dir=/tmp/chrome-profile")
    opts.add_argument("--profile-directory=Default")

    # ✅ stealth
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)

    opts.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36"
    )

    drv = webdriver.Chrome(service=Service(CHROME_DRIVER_PATH), options=opts)

    # hide webdriver flag
    drv.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

    return drv


def ensure_driver():
    global driver
    if driver is None:
        driver = create_driver()
    return driver


def restart_driver():
    global driver
    if driver:
        try:
            driver.quit()
            log("♻️ Driver restarted")
        except:
            pass
    driver = None


# ---------------- SCRAPER ---------------- #
def get_values(drv):
    try:
        els = drv.find_elements(By.CSS_SELECTOR, "[class*='valueValue']")
        return [e.text.strip() for e in els if e.text.strip()]
    except Exception as e:
        log(f"⚠️ get_values error: {e}")
        return []


def keep_alive(drv):
    try:
        drv.execute_script("window.localStorage.setItem('keepAlive', Date.now());")
    except:
        pass


def scrape_day(url):
    if not url:
        log("❌ Empty URL")
        return [""] * EXPECTED_COUNT, "NOT OK", "", ""

    for attempt in range(2):
        try:
            drv = ensure_driver()

            log(f"🌍 Opening: {url}")
            drv.get(url)

            time.sleep(random.uniform(2, 4))  # human delay

            current_url = drv.current_url
            title = drv.title

            log(f"📍 URL: {current_url}")
            log(f"📄 Title: {title}")

            # 🔐 session expired handling
            if "login" in current_url.lower():
                log("🔐 Session expired → reloading homepage")

                drv.get("https://in.tradingview.com/")
                time.sleep(5)

                drv.get(url)
                time.sleep(3)

            # wait for data
            try:
                WebDriverWait(drv, 15).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "[class*='valueValue']"))
                )
                log("✅ Data element found")
            except:
                log("⚠️ Data element NOT found")

            vals = get_values(drv)
            log(f"🔢 Values: {vals}")

            # scroll fallback
            if len(vals) < EXPECTED_COUNT:
                for y in [600, 1200, 2000]:
                    drv.execute_script(f"window.scrollTo(0, {y});")
                    time.sleep(1.5)

                    new_vals = get_values(drv)
                    log(f"📊 After scroll {y}: {new_vals}")

                    if len(new_vals) > len(vals):
                        vals = new_vals

                    if len(vals) >= EXPECTED_COUNT:
                        break

            count = len(vals)
            log(f"📊 Found {count}/{EXPECTED_COUNT}")

            keep_alive(drv)

            if count >= EXPECTED_COUNT:
                return vals[:EXPECTED_COUNT], "OK", url, current_url
            else:
                return (vals + [""] * EXPECTED_COUNT)[:EXPECTED_COUNT], "NOT OK", url, current_url

        except Exception as e:
            log(f"❌ Attempt {attempt+1} failed: {e}")
            restart_driver()

    log("🚨 Completely failed")
    return [""] * EXPECTED_COUNT, "NOT OK", url, ""


# ---------------- GOOGLE SHEETS ---------------- #
def connect_sheets():
    gc = gspread.service_account("credentials.json")
    sh_main = gc.open("STOCKLIST 2").worksheet("Sheet1")
    sh_data = gc.open("Tradingview Data Reel Experimental May").worksheet("Sheet5")
    return sh_main, sh_data


def process_row(i, company_list, url_list, today):
    name = company_list[i].strip()
    url = url_list[i].strip() if i < len(url_list) and "http" in url_list[i] else None

    log(f"🔍 [{i+1}] {name}")

    vals, status, sheet_url, browser_url = scrape_day(url)

    row = i + 1
    payload = [
        {"range": f"A{row}", "values": [[name]]},
        {"range": f"B{row}", "values": [[today]]},
        {"range": f"C{row}:I{row}", "values": [vals]},
        {"range": f"J{row}", "values": [[status]]},
        {"range": f"K{row}", "values": [[sheet_url]]},
        {"range": f"L{row}", "values": [[browser_url]]}
    ]

    return payload, (status == "OK")


# ---------------- MAIN ---------------- #
try:
    sheet_main, sheet_data = connect_sheets()
    company_list = sheet_main.col_values(1)
    url_list = sheet_main.col_values(5)

    log("✅ Sheet connected")

except Exception as e:
    log(f"❌ Sheet error: {e}")
    sys.exit(1)

batch = []
retry = []
today = date.today().strftime("%m/%d/%Y")

for i in range(START_ROW, min(END_ROW, len(company_list))):
    payload, ok = process_row(i, company_list, url_list, today)
    batch.extend(payload)

    if not ok:
        retry.append(i)

    time.sleep(random.uniform(2, 5))  # 🔥 anti-block delay

    if (i + 1) % RESTART_EVERY_ROWS == 0:
        restart_driver()

    if len(batch) >= BATCH_SIZE * 6:
        log("🚀 Uploading batch")
        sheet_data.batch_update(batch, value_input_option="RAW")
        batch = []

if batch:
    sheet_data.batch_update(batch, value_input_option="RAW")

# retry failed
if retry:
    log(f"🔁 Retrying {len(retry)} failed rows")
    restart_driver()

    for i in retry:
        payload, _ = process_row(i, company_list, url_list, today)
        sheet_data.batch_update(payload, value_input_option="RAW")

restart_driver()
log("🏁 DONE")
