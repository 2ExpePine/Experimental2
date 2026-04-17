import sys
import os
import time
import json
from datetime import date
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
# Fix: Ensure you have webdriver_manager installed
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException, WebDriverException
from bs4 import BeautifulSoup
import gspread

# Force immediate log output
def log(msg):
    print(msg, flush=True)

# ---------------- CONFIG & SHARDING ---------------- #
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_STEP = int(os.getenv("SHARD_STEP", "1"))

checkpoint_file = os.getenv("CHECKPOINT_FILE", f"checkpoint_{SHARD_INDEX}.txt")
last_i = int(open(checkpoint_file).read()) if os.path.exists(checkpoint_file) else 0

# ---------------- BROWSER FACTORY ---------------- #
def create_driver():
    log("🌐 Initializing Hardened Chrome Instance...")
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--blink-settings=imagesEnabled=false")
    opts.add_experimental_option('excludeSwitches', ['enable-logging'])
    opts.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=opts
    )
    driver.set_page_load_timeout(40)

    if os.path.exists("cookies.json"):
        try:
            driver.get("https://in.tradingview.com/")
            time.sleep(3)
            with open("cookies.json", "r") as f:
                cookies = json.load(f)
            for c in cookies:
                try:
                    driver.add_cookie({
                        k: v for k, v in c.items()
                        if k in ("name", "value", "path", "secure", "expiry")
                    })
                except:
                    continue
            driver.refresh()
            time.sleep(2)
            log("✅ Cookies applied successfully")
        except Exception as e:
            log(f"⚠️ Cookie error: {str(e)[:60]}")

    return driver

# ---------------- SCRAPER LOGIC ---------------- #
def scrape_tradingview(driver, url):
    try:
        log(f"🔗 Visiting URL: {url}")
        driver.get(url)
        
        # Wait for the main data container
        target_xpath = '/html/body/div[2]/div/div[5]/div/div[1]/div/div[2]/div[1]/div[2]/div/div[1]/div[2]/div[2]/div[2]/div[2]/div'
        try:
            WebDriverWait(driver, 45).until(
                EC.visibility_of_element_located((By.XPATH, target_xpath))
            )
        except TimeoutException:
            log(f"❌ Timeout: Data element not found at {url}")
            return []

        soup = BeautifulSoup(driver.page_source, "html.parser")
        # Find all values based on the class provided
        value_elements = soup.find_all("div", class_="valueValue-l31H9iuA apply-common-tooltip")
        
        if not value_elements:
            log(f"⚠️ No data elements found with specified class on {url}")
            return []

        values = [
            el.get_text().replace('−', '-').replace('∅', 'None')
            for el in value_elements
        ]
        return values

    except WebDriverException as e:
        log(f"🛑 Browser Crash or Connection Error: {str(e)[:100]}")
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
BATCH_SIZE = 50

try:
    for i in range(last_i, len(company_list)):
        if i % SHARD_STEP != SHARD_INDEX:
            continue
        if i >= 2500:
            break

        url = company_list[i]
        name = name_list[i] if i < len(name_list) else f"Row {i}"

        # Logic check for empty URLs
        if not url or str(url).strip() == "":
            log(f"⏩ Row {i}: Skipping because URL is empty.")
            continue

        log(f"--- Processing [{i}] {name} ---")

        values = scrape_tradingview(driver, url)

        if values == "RESTART":
            log(f"🔄 Restarting driver for: {name}")
            try:
                driver.quit()
            except:
                pass
            driver = create_driver()
            values = scrape_tradingview(driver, url)
            if values == "RESTART":
                log(f"❌ Failed again after restart for {name}")
                values = []

        if isinstance(values, list) and values:
            target_row = i + 1
            batch_list.append({
                "range": f"A{target_row}",
                "values": [[name, current_date] + values]
            })
            log(f"✅ Scraped {len(values)} metrics for {name}")
            log(f"📦 Buffered ({len(batch_list)}/{BATCH_SIZE})")
        else:
            log(f"⏭️ SKIPPED: {name} (Reason: No data extracted or Timeout)")

        if len(batch_list) >= BATCH_SIZE:
            try:
                sheet_data.batch_update(batch_list)
                log(f"🚀 Saved {len(batch_list)} rows to Google Sheets")
                batch_list = []
            except Exception as e:
                log(f"⚠️ API Error: {e}")
                if "429" in str(e):
                    log("⏳ Quota hit, sleeping 60s...")
                    time.sleep(60)

        with open(checkpoint_file, "w") as f:
            f.write(str(i + 1))

        time.sleep(1) # Slightly increased delay to help stability

finally:
    if batch_list:
        try:
            sheet_data.batch_update(batch_list)
            log(f"✅ Final save: {len(batch_list)} rows")
        except:
            pass
    driver.quit()
    log("🏁 Scraping session ended.")
