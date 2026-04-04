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

def log(msg):
    t = time.strftime("%H:%M:%S")
    print(f"[{t}] {msg}", flush=True)

# ---------------- CONFIG ---------------- #
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_SIZE = int(os.getenv("SHARD_SIZE", "500"))
START_ROW = SHARD_INDEX * SHARD_SIZE
END_ROW = START_ROW + SHARD_SIZE
checkpoint_file = os.getenv("CHECKPOINT_FILE", f"checkpoint_day_{SHARD_INDEX}.txt")

EXPECTED_COUNT = 5   # ✅ ONLY 5 VALUES
BATCH_SIZE = 5
RESTART_EVERY_ROWS = 20
COOKIE_FILE = os.getenv("COOKIE_FILE", "cookies.json")
CHROME_DRIVER_PATH = ChromeDriverManager().install()

DAY_OUTPUT_START_COL = 3  # Column C

# ---------------- UTILS ---------------- #
def col_num_to_letter(n):
    result = ""
    while n > 0:
        n, rem = divmod(n - 1, 26)
        result = chr(65 + rem) + result
    return result

DAY_START_COL_LETTER = col_num_to_letter(DAY_OUTPUT_START_COL)
DAY_END_COL_LETTER = col_num_to_letter(DAY_OUTPUT_START_COL + EXPECTED_COUNT - 1)

STATUS_COL = col_num_to_letter(DAY_OUTPUT_START_COL + EXPECTED_COUNT)
SHEET_URL_COL = col_num_to_letter(DAY_OUTPUT_START_COL + EXPECTED_COUNT + 1)
BROWSER_URL_COL = col_num_to_letter(DAY_OUTPUT_START_COL + EXPECTED_COUNT + 2)

def api_retry(func, *args, **kwargs):
    for attempt in range(5):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            wait = (2 ** attempt) + random.random()
            log(f"⚠️ API Issue: {str(e)[:100]}. Retrying in {wait:.1f}s...")
            time.sleep(wait)
    return func(*args, **kwargs)

# ---------------- STATE ---------------- #
if os.path.exists(checkpoint_file):
    try:
        last_i = max(int(open(checkpoint_file).read().strip()), START_ROW)
    except:
        last_i = START_ROW
else:
    last_i = START_ROW

# ---------------- DRIVER ---------------- #
driver = None

def create_driver():
    log(f"🌐 [DAY Shard {SHARD_INDEX}] Initializing browser...")
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--incognito")

    drv = webdriver.Chrome(service=Service(CHROME_DRIVER_PATH), options=opts)
    drv.set_page_load_timeout(60)

    # Load cookies (optional)
    if os.path.exists(COOKIE_FILE):
        try:
            drv.get("https://in.tradingview.com/")
            with open(COOKIE_FILE, "r", encoding="utf-8") as f:
                cookies = json.load(f)

            for c in cookies:
                try:
                    drv.add_cookie({
                        "name": c["name"],
                        "value": c["value"],
                        "path": c.get("path", "/"),
                    })
                except:
                    continue

            drv.refresh()
            time.sleep(3)
        except:
            pass

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
        except:
            pass
    driver = None

# ---------------- SCRAPER ---------------- #
def get_values(drv):
    try:
        vals = []

        elements = drv.find_elements(By.CSS_SELECTOR, "div[data-name='legend-value']")

        for el in elements:
            txt = el.text.strip()
            if txt:
                vals.append(txt)

        log(f"   Found {len(vals)} values -> {vals}")
        return vals

    except Exception as e:
        log(f"   get_values error: {e}")
        return []

def scrape_day(url):
    if not url:
        return [""] * EXPECTED_COUNT, "Bad URL", "", ""

    for attempt in range(2):
        try:
            drv = ensure_driver()
            drv.get(url)

            wait = WebDriverWait(drv, 20)
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div[data-name='legend']")))
            time.sleep(6)

            vals = get_values(drv)

            browser_url = drv.current_url

            # Take only first 5 values
            vals = vals[:EXPECTED_COUNT]

            # Pad if less than 5
            if len(vals) < EXPECTED_COUNT:
                vals += [""] * (EXPECTED_COUNT - len(vals))

            return vals, f"{len(vals)} Values", url, browser_url

        except Exception as e:
            log(f"   ❌ Attempt {attempt + 1} Failed: {str(e)[:100]}")
            restart_driver()

    return [""] * EXPECTED_COUNT, "Failed", url, ""

# ---------------- SHEETS ---------------- #
def connect_sheets():
    gc = gspread.service_account("credentials.json")
    sh_main = gc.open("Stock List").worksheet("Sheet1")
    sh_data = gc.open("Tradingview Data Reel Experimental May").worksheet("Sheet5")
    return sh_main, sh_data

try:
    sheet_main, sheet_data = connect_sheets()
    company_list = api_retry(sheet_main.col_values, 1)
    url_list = api_retry(sheet_main.col_values, 5)  # ✅ Column E

    log(f"✅ Ready. Processing Rows {last_i + 1} to {min(END_ROW, len(company_list))}")

except Exception as e:
    log(f"❌ Initial Connection Error: {e}")
    sys.exit(1)

batch_list = []
current_date = date.today().strftime("%m/%d/%Y")

try:
    loop_end = min(END_ROW, len(company_list))

    for i in range(last_i, loop_end):
        name = company_list[i].strip() if i < len(company_list) else ""
        url = url_list[i].strip() if i < len(url_list) and "http" in url_list[i] else None

        log(f"🔍 [{i + 1}/{loop_end}] {name}")

        vals, status, sheet_url_used, browser_url_used = scrape_day(url)

        row_idx = i + 1

        batch_list.append({"range": f"A{row_idx}", "values": [[name]]})
        batch_list.append({"range": f"B{row_idx}", "values": [[current_date]]})
        batch_list.append({
            "range": f"{DAY_START_COL_LETTER}{row_idx}:{DAY_END_COL_LETTER}{row_idx}",
            "values": [vals]
        })
        batch_list.append({"range": f"{STATUS_COL}{row_idx}", "values": [[status]]})
        batch_list.append({"range": f"{SHEET_URL_COL}{row_idx}", "values": [[url]]})
        batch_list.append({"range": f"{BROWSER_URL_COL}{row_idx}", "values": [[browser_url_used]]})

        with open(checkpoint_file, "w") as f:
            f.write(str(i + 1))

        if (i + 1) % RESTART_EVERY_ROWS == 0:
            restart_driver()

        if len(batch_list) // 6 >= BATCH_SIZE:
            log("🚀 Uploading batch...")
            api_retry(sheet_data.batch_update, batch_list, value_input_option="RAW")
            batch_list = []

finally:
    if batch_list:
        log("🚀 Uploading final batch...")
        api_retry(sheet_data.batch_update, batch_list, value_input_option="RAW")

    restart_driver()
    log("🏁 COMPLETED.")
