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
from selenium.common.exceptions import NoSuchElementException, TimeoutException, WebDriverException

from bs4 import BeautifulSoup
import gspread
from webdriver_manager.chrome import ChromeDriverManager


# ---------------- LOGGER ---------------- #
def log(msg):
    print(msg, flush=True)


# ---------------- CONFIG ---------------- #
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_STEP = int(os.getenv("SHARD_STEP", "1"))

checkpoint_file = os.getenv("CHECKPOINT_FILE", f"checkpoint_{SHARD_INDEX}.txt")
last_i = int(open(checkpoint_file).read()) if os.path.exists(checkpoint_file) else 0


# ---------------- BROWSER ---------------- #
def create_driver():
    log("🌐 Launching Chrome...")

    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")

    # Anti-block tweaks
    opts.add_argument("--disable-blink-features=AutomationControlled")

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

    # Hide automation flag
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

    # ---- Cookies ---- #
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
            log("✅ Cookies loaded")

        except Exception as e:
            log(f"⚠️ Cookie error: {str(e)[:80]}")

    return driver


# ---------------- SCRAPER ---------------- #
def scrape_tradingview(driver, url, index):
    try:
        driver.get(url)

        # wait for ANY value-like element
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((
                By.XPATH,
                "//div[contains(@class,'valueValue')]"
            ))
        )

        soup = BeautifulSoup(driver.page_source, "html.parser")

        values = [
            el.get_text().replace('−', '-').replace('∅', 'None')
            for el in soup.select("div[class*='valueValue']")
        ]

        if not values:
            log("⚠️ No values found → Possible block / layout change")
            driver.save_screenshot(f"debug_no_values_{index}.png")
            return "NO_DATA"

        return values

    except TimeoutException:
        log("⏱️ Timeout → Page slow / blocked")
        driver.save_screenshot(f"debug_timeout_{index}.png")
        return "TIMEOUT"

    except NoSuchElementException:
        log("❌ Element not found → Selector issue")
        return "NO_ELEMENT"

    except WebDriverException as e:
        log(f"🛑 Browser crash: {str(e)[:80]}")
        return "RESTART"

    except Exception as e:
        log(f"🔥 Unknown error: {str(e)[:80]}")
        return "ERROR"


# ---------------- SETUP ---------------- #
log("📊 Connecting to Google Sheets...")

try:
    gc = gspread.service_account("credentials.json")

    sheet_main = gc.open("Stock List").worksheet("Sheet1")
    sheet_data = gc.open("Tradingview Data Reel Experimental May").worksheet("Sheet5")

    company_list = sheet_main.col_values(5)
    name_list = sheet_main.col_values(1)

    current_date = date.today().strftime("%m/%d/%Y")

    log(f"✅ Setup done | Shard {SHARD_INDEX} | Resume {last_i}")

except Exception as e:
    log(f"❌ Setup error: {e}")
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

        log(f"\n🔍 [{i}] {name}")
        log(f"🌐 URL: {url}")

        values = scrape_tradingview(driver, url, i)

        # -------- HANDLE STATES -------- #
        if values == "RESTART":
            log("🔄 Restarting browser...")
            try:
                driver.quit()
            except:
                pass

            driver = create_driver()
            continue

        elif values == "TIMEOUT":
            log(f"⚠️ Skipped (Timeout): {name}")
            continue

        elif values == "NO_ELEMENT":
            log(f"⚠️ Skipped (Selector issue): {name}")
            continue

        elif values == "NO_DATA":
            log(f"⚠️ Skipped (Blocked / Empty): {name}")
            continue

        elif values == "ERROR":
            log(f"⚠️ Skipped (Unknown error): {name}")
            continue

        # -------- SUCCESS -------- #
        if isinstance(values, list) and values:
            target_row = i + 1

            batch_list.append({
                "range": f"A{target_row}",
                "values": [[name, current_date] + values]
            })

            log(f"✅ Data extracted ({len(values)} values)")
            log(f"📦 Buffer: {len(batch_list)}/{BATCH_SIZE}")

        else:
            log(f"⚠️ Unexpected empty result: {name}")
            continue

        # -------- SAVE -------- #
        if len(batch_list) >= BATCH_SIZE:
            try:
                sheet_data.batch_update(batch_list)
                log(f"🚀 Saved batch of {len(batch_list)}")
                batch_list = []

            except Exception as e:
                log(f"⚠️ API error: {e}")

                if "429" in str(e):
                    log("⏳ Rate limit hit → sleeping 60s")
                    time.sleep(60)

        # -------- CHECKPOINT -------- #
        with open(checkpoint_file, "w") as f:
            f.write(str(i + 1))

        time.sleep(1)


finally:
    if batch_list:
        try:
            sheet_data.batch_update(batch_list)
            log(f"✅ Final save: {len(batch_list)} rows")
        except:
            pass

    driver.quit()
    log("🏁 Scraping completed")
