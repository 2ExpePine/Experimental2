import sys
import os
import time
import json
from datetime import date
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
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
    opts.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=opts
    )
    driver.set_page_load_timeout(45)

    if os.path.exists("cookies.json"):
        try:
            driver.get("https://in.tradingview.com/")
            time.sleep(4)
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
            log("✅ Cookies applied successfully")
        except Exception as e:
            log(f"⚠️ Cookie error: {str(e)[:50]}")

    return driver

# ---------------- SCRAPER LOGIC ---------------- #
def scrape_tradingview(driver, url):
    # Classes identified from your screenshot
    container_class = "valuesAdditionalWrapper-l31H9iuA"
    item_class = "valueItem-l31H9iuA"

    try:
        log(f"🔗 Visiting: {url}")
        driver.get(url)
        
        # Wait for the main container from your screenshot
        try:
            WebDriverWait(driver, 45).until(
                EC.presence_of_element_located((By.CLASS_NAME, container_class))
            )
            # Short sleep to ensure all 5 values have loaded their text
            time.sleep(2)
        except TimeoutException:
            log(f"❌ Timeout: Container {container_class} not found at {url}")
            return []

        soup = BeautifulSoup(driver.page_source, "html.parser")
        
        # Target the specific wrapper shown in your image
        wrapper = soup.find("div", class_=container_class)
        
        if not wrapper:
            log(f"⚠️ XPath element found but BS4 could not find {container_class}")
            return []

        # Find the 5 items inside that wrapper
        items = wrapper.find_all("div", class_=item_class)
        
        extracted_values = []
        for item in items:
            # Clean the text (handle TradingView's special minus and empty symbols)
            val = item.get_text(strip=True).replace('−', '-').replace('∅', 'None')
            extracted_values.append(val)

        if extracted_values:
            log(f"✅ Found {len(extracted_values)} values: {extracted_values}")
            return extracted_values
        else:
            log("❌ No text values found inside the items.")
            return []

    except WebDriverException as e:
        log(f"🛑 Browser Error: {str(e)[:50]}")
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
    log(f"✅ Setup complete | Resume index {last_i}")
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

        if not url or "http" not in str(url):
            log(f"⏩ Row {i} skipped: Invalid URL")
            continue

        log(f"--- [{i}] Processing {name} ---")
        values = scrape_tradingview(driver, url)

        # Handle browser restarts
        if values == "RESTART":
            try: driver.quit()
            except: pass
            driver = create_driver()
            values = scrape_tradingview(driver, url)
            if values == "RESTART": values = []

        # If we got exactly the data we wanted
        if isinstance(values, list) and values:
            target_row = i + 1
            batch_list.append({
                "range": f"A{target_row}",
                "values": [[name, current_date] + values]
            })
            log(f"📦 Buffered {name} ({len(values)} metrics)")
        else:
            log(f"⏭️ SKIPPED: {name} (Reason: Data missing or Timeout)")

        # Batch update to Google Sheets
        if len(batch_list) >= BATCH_SIZE:
            try:
                sheet_data.batch_update(batch_list)
                log(f"🚀 Saved {len(batch_list)} rows to Sheets")
                batch_list = []
            except Exception as e:
                log(f"⚠️ API Error: {e}")
                time.sleep(60)

        # Save progress
        with open(checkpoint_file, "w") as f:
            f.write(str(i + 1))

        time.sleep(1.5)

finally:
    if batch_list:
        try:
            sheet_data.batch_update(batch_list)
            log(f"✅ Final save of {len(batch_list)} rows completed.")
        except Exception as e:
            log(f"❌ Final save failed: {e}")
    
    driver.quit()
    log("🏁 Scraping session finished.")
