from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from bs4 import BeautifulSoup
import gspread
from datetime import date
import os
import time
import json
import random
from webdriver_manager.chrome import ChromeDriverManager

# ---------------- SHARDING ---------------- #
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_STEP = int(os.getenv("SHARD_STEP", "1"))
checkpoint_file = os.getenv("CHECKPOINT_FILE", f"checkpoint_{SHARD_INDEX}.txt")
last_i = int(open(checkpoint_file).read()) if os.path.exists(checkpoint_file) else 1

# ---------------- CHROME SETUP ---------------- #
chrome_options = Options()
chrome_options.add_argument("--headless=new")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--remote-debugging-port=9222")
chrome_options.add_argument(
    "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)

# ---------------- GOOGLE SHEETS AUTH ---------------- #
gc = gspread.service_account("credentials.json")
sheet_main = gc.open('Stock List').worksheet('Sheet1')
sheet_data = gc.open('Tradingview Data Reel Experimental May').worksheet('Sheet5')

company_list = sheet_main.col_values(5)
name_list = sheet_main.col_values(1)
current_date = date.today().strftime("%m/%d/%Y")

# ---------------- CUSTOM EXPECTED CONDITION ---------------- #
class text_content_loaded:
    def __init__(self, locator, min_count=1):
        self.locator = locator
        self.min_count = min_count

    def __call__(self, driver):
        elements = driver.find_elements(*self.locator)
        non_empty = [el for el in elements if el.text.strip()]
        return elements if len(non_empty) >= self.min_count else False

# ---------------- SCRAPER ---------------- #
def scrape_tradingview(driver, company_url):
    DATA_LOCATOR = (By.CLASS_NAME, "valueValue-l31H9iuA")

    try:
        driver.get(company_url)
        WebDriverWait(driver, 75).until(
            text_content_loaded(DATA_LOCATOR, min_count=10)
        )

        soup = BeautifulSoup(driver.page_source, "html.parser")
        values = [
            el.get_text().replace('−', '-').replace('∅', 'None')
            for el in soup.find_all(
                "div", class_="valueValue-l31H9iuA apply-common-tooltip"
            )
        ]
        return values

    except Exception:
        return []

# ---------------- DRIVER INIT ---------------- #
driver = webdriver.Chrome(
    service=Service(ChromeDriverManager().install()),
    options=chrome_options
)

# ---------------- LOAD COOKIES ---------------- #
if os.path.exists("cookies.json"):
    driver.get("https://www.tradingview.com/")
    with open("cookies.json", "r", encoding="utf-8") as f:
        for c in json.load(f):
            try:
                driver.add_cookie({
                    k: c[k] for k in c
                    if k in ("name", "value", "domain", "path", "expiry")
                })
            except:
                pass
    driver.refresh()
    time.sleep(2)

# ---------------- MAIN LOOP ---------------- #
BATCH_SIZE = 50
buffer = []
start_row = None   # <-- FIX: remembers where this batch starts

for i, company_url in enumerate(company_list[last_i:], last_i):

    if i % SHARD_STEP != SHARD_INDEX:
        continue

    if i > 2500:
        break

    name = name_list[i] if i < len(name_list) else f"Row {i}"
    print(f"Scraping {i}: {name}")

    values = scrape_tradingview(driver, company_url)
    row_data = [name, current_date] + values if values else ["ERROR"]

    sheet_row = i + 2  # Header offset

    if not buffer:
        start_row = sheet_row

    buffer.append(row_data)

    with open(checkpoint_file, "w") as f:
        f.write(str(i))

    # -------- ORDER-SAFE WRITE -------- #
    if len(buffer) >= BATCH_SIZE:
        end_row = start_row + len(buffer) - 1
        range_name = f"A{start_row}:ZZ{end_row}"
        sheet_data.update(range_name, buffer, value_input_option="RAW")
        print(f"✅ Written rows {start_row}–{end_row}")
        buffer.clear()

    time.sleep(1.5 + random.random() * 1.5)

# ---------------- FINAL FLUSH ---------------- #
if buffer:
    end_row = start_row + len(buffer) - 1
    range_name = f"A{start_row}:ZZ{end_row}"
    sheet_data.update(range_name, buffer, value_input_option="RAW")
    print(f"✅ Final write rows {start_row}–{end_row}")

driver.quit()
print("All done ✅")
