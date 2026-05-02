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

# ---------------- LOGGING ---------------- #
def log(msg):
    t = time.strftime("%H:%M:%S")
    print(f"[{t}] {msg}", flush=True)

# ---------------- CONFIG ---------------- #
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_SIZE = int(os.getenv("SHARD_SIZE", "500"))
START_ROW = SHARD_INDEX * SHARD_SIZE
END_ROW = START_ROW + SHARD_SIZE
checkpoint_file = os.getenv("CHECKPOINT_FILE", f"checkpoint_day_{SHARD_INDEX}.txt")

EXPECTED_COUNT = 7
BATCH_SIZE = 50 
RESTART_EVERY_ROWS = 20
COOKIE_FILE = os.getenv("COOKIE_FILE", "cookies.json")
CHROME_DRIVER_PATH = ChromeDriverManager().install()

DAY_OUTPUT_START_COL = 3  

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
            log(f"⚠️ API Issue: {str(e)[:80]} | retry in {wait:.1f}s")
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
    log(f"🌐 [Shard {SHARD_INDEX}] Initializing browser...")

    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--disable-infobars")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--remote-debugging-port=9222")

    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36")

    drv = webdriver.Chrome(service=Service(CHROME_DRIVER_PATH), options=opts)

    # Load cookies
    if os.path.exists(COOKIE_FILE):
        try:
            drv.get("https://in.tradingview.com/")
            with open(COOKIE_FILE, "r") as f:
                cookies = json.load(f)

            for c in cookies:
                drv.add_cookie({k: v for k, v in c.items() if k in ("name", "value", "path", "secure", "expiry")})

            drv.refresh()
            log("🍪 Cookies loaded successfully")
            time.sleep(2)
        except Exception as e:
            log(f"⚠️ Cookie load failed: {e}")

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
        elements = drv.find_elements(By.CSS_SELECTOR, "[class*='valueValue']")
        return [el.text.strip() for el in elements if el.text.strip()]
    except Exception as e:
        log(f"⚠️ get_values error: {e}")
        return []

def scrape_day(url):
    if not url:
        log("❌ No URL provided")
        return [""] * EXPECTED_COUNT, "NOT OK", "", ""

    for attempt in range(2):
        try:
            drv = ensure_driver()

            log(f"🌍 Opening: {url}")
            drv.get(url)

            log("⏳ Waiting for elements...")
            try:
                WebDriverWait(drv, 20).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "[class*='valueValue']"))
                )
                log("✅ Element found")
            except:
                log("⚠️ Element NOT found (timeout)")

            time.sleep(3)

            current_url = drv.current_url
            title = drv.title

            log(f"📍 Final URL: {current_url}")
            log(f"📄 Title: {title}")

            if "login" in current_url.lower():
                log("🚨 Redirected to LOGIN page!")

            vals = get_values(drv)
            log(f"🔢 Values: {vals}")

            if len(vals) < EXPECTED_COUNT:
                log("🔄 Scrolling to load data...")
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

def process_row(i, company_list, url_list, current_date):
    name = company_list[i].strip()
    url = url_list[i].strip() if i < len(url_list) and "http" in url_list[i] else None

    log(f"🔍 [{i+1}] {name}")

    vals, status, sheet_url, browser_url = scrape_day(url)

    row = i + 1
    payload = [
        {"range": f"A{row}", "values": [[name]]},
        {"range": f"B{row}", "values": [[current_date]]},
        {"range": f"{DAY_START_COL_LETTER}{row}:{DAY_END_COL_LETTER}{row}", "values": [vals]},
        {"range": f"{STATUS_COL}{row}", "values": [[status]]},
        {"range": f"{SHEET_URL_COL}{row}", "values": [[sheet_url]]},
        {"range": f"{BROWSER_URL_COL}{row}", "values": [[browser_url]]}
    ]

    return payload, (status == "OK")

# ---------------- MAIN ---------------- #
try:
    sheet_main, sheet_data = connect_sheets()
    company_list = api_retry(sheet_main.col_values, 1)
    url_list = api_retry(sheet_main.col_values, 5)

    log(f"✅ Starting rows {last_i+1} to {min(END_ROW, len(company_list))}")

except Exception as e:
    log(f"❌ Sheet connection failed: {e}")
    sys.exit(1)

retry_indices = []
batch = []
current_date = date.today().strftime("%m/%d/%Y")

for i in range(last_i, min(END_ROW, len(company_list))):
    payload, success = process_row(i, company_list, url_list, current_date)
    batch.extend(payload)

    if not success:
        retry_indices.append(i)

    with open(checkpoint_file, "w") as f:
        f.write(str(i + 1))

    if (i + 1) % RESTART_EVERY_ROWS == 0:
        restart_driver()

    if len(batch) // 6 >= BATCH_SIZE:
        log("🚀 Uploading batch...")
        api_retry(sheet_data.batch_update, batch, value_input_option="RAW")
        batch = []

if batch:
    api_retry(sheet_data.batch_update, batch, value_input_option="RAW")

# ---------------- RETRY ---------------- #
if retry_indices:
    log(f"🔁 Retrying {len(retry_indices)} failed rows...")
    restart_driver()

    batch = []
    for idx, i in enumerate(retry_indices):
        payload, _ = process_row(i, company_list, url_list, current_date)
        batch.extend(payload)

        if (idx + 1) % 10 == 0:
            restart_driver()
            api_retry(sheet_data.batch_update, batch, value_input_option="RAW")
            batch = []

    if batch:
        api_retry(sheet_data.batch_update, batch, value_input_option="RAW")

restart_driver()
log("🏁 SCRAPING COMPLETED")
