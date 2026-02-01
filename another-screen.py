import os, time, json, gspread, concurrent.futures, re, socket, threading
import pandas as pd
import mysql.connector
from mysql.connector import pooling
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from datetime import datetime

# ---------------- CONFIG ---------------- #
SPREADSHEET_NAME = "Stock List"
TAB_NAME = "Weekday"
MAX_THREADS = 4

progress_lock = threading.Lock()
processed_count = 0
total_rows = 0

db_pool = None

# ---------------- DIAGNOSTIC HELPERS ---------------- #

def _mask(v, show=6):
    """Mask secrets for safe logs."""
    if v is None or str(v).strip() == "":
        return "❌ MISSING"
    s = str(v)
    if len(s) <= show:
        return "*" * len(s)
    return s[:show] + "..." + "*" * 4

def _split_host_port(raw_host: str):
    """
    Supports:
      - DB_HOST="host"
      - DB_HOST="host:3306"
      - DB_HOST="127.0.0.1"
    If DB_PORT is set, it overrides unless DB_HOST includes :port.
    """
    raw_host = (raw_host or "").strip()
    raw_port = (os.getenv("DB_PORT") or "").strip()

    host = raw_host
    port = 3306

    # If DB_HOST has host:port (and only one colon), split
    if ":" in raw_host and raw_host.count(":") == 1 and not raw_host.startswith("["):
        h, p = raw_host.split(":")
        host = h.strip()
        try:
            port = int(p.strip())
        except:
            port = 3306
    else:
        if raw_port:
            try:
                port = int(raw_port)
            except:
                port = 3306

    return host, port

def _tcp_check(host, port, timeout=5):
    """Checks if host:port is reachable at TCP level."""
    try:
        socket.create_connection((host, int(port)), timeout=timeout).close()
        return True, "TCP reachable (port open)"
    except Exception as e:
        return False, f"TCP NOT reachable: {e}"

def build_db_config():
    host, port = _split_host_port(os.getenv("DB_HOST"))
    return {
        "host": host,
        "port": port,
        "user": (os.getenv("DB_USER") or "").strip(),
        "password": os.getenv("DB_PASSWORD") or "",
        "database": (os.getenv("DB_NAME") or "").strip(),
        "connect_timeout": 30,
    }

def explain_mysql_error(err: Exception, cfg: dict, tcp_ok: bool):
    """
    Converts common MySQL errors into human-readable reasons + fixes.
    """
    msg = str(err)

    # MySQL connector errors often have err.errno
    errno = getattr(err, "errno", None)

    tips = []
    tips.append(f"Host={cfg['host']} Port={cfg['port']} User={cfg['user']} DB={cfg['database']}")

    # Missing env vars
    if not cfg["host"] or not cfg["user"] or not cfg["database"] or not cfg["password"]:
        missing = []
        if not cfg["host"]: missing.append("DB_HOST")
        if not cfg["user"]: missing.append("DB_USER")
        if not cfg["database"]: missing.append("DB_NAME")
        if not cfg["password"]: missing.append("DB_PASSWORD")
        return (
            "❌ Secrets/env vars missing",
            f"Missing: {', '.join(missing)}. Copy secrets to the NEW repo/runtime and ensure names match exactly."
        )

    # TCP unreachable -> classic "refused / timed out"
    if not tcp_ok:
        return (
            "❌ Network/port blocked or wrong host",
            "Your script cannot reach the DB server at TCP level.\n"
            "Fix: verify DB_HOST/DB_PORT, open inbound port 3306 (firewall/security group), enable Remote MySQL, "
            "and ensure MySQL is listening on the server (not only localhost)."
        )

    # Now TCP is OK, but MySQL handshake/auth failed -> credentials, user permissions, SSL
    if errno in (1045,):
        return (
            "❌ Access denied (wrong user/password OR user not allowed from this host)",
            "Fix: verify DB_USER/DB_PASSWORD. Also ensure MySQL user has permission to connect from this machine/IP "
            "(e.g., user@'%' or user@'your-server-ip')."
        )

    if errno in (1049,):
        return (
            "❌ Unknown database",
            "Fix: DB_NAME is wrong or DB not created. Check the database name exactly."
        )

    if errno in (2003, 2006, 2013):
        return (
            "❌ Connection issue (MySQL layer)",
            "TCP is reachable but MySQL connection still fails.\n"
            "Fix: MySQL may be rejecting remote clients, requiring SSL, or max connections reached. "
            "Check MySQL bind-address, remote access settings, and server logs."
        )

    if "SSL" in msg.upper() or errno in (2026,):
        return (
            "❌ SSL required / SSL handshake issue",
            "Fix: your MySQL server requires SSL. Add ssl_disabled=True if allowed, OR configure SSL certs properly."
        )

    return (
        "❌ Unknown MySQL error",
        f"Raw error: {msg}\nFix: double-check host/port and server settings. If using shared hosting, enable Remote MySQL."
    )

def init_db_pool():
    """
    Creates db_pool AND prints a precise diagnosis on failure.
    """
    global db_pool
    cfg = build_db_config()

    print("\n================ DB DIAGNOSTIC ================")
    print("DB_HOST:", _mask(os.getenv("DB_HOST"), 18))
    print("DB_PORT:", _mask(os.getenv("DB_PORT"), 6))
    print("DB_USER:", _mask(os.getenv("DB_USER"), 4))
    print("DB_NAME:", _mask(os.getenv("DB_NAME"), 4))
    print("DB_PASSWORD:", "✅ SET" if (os.getenv("DB_PASSWORD") or "") else "❌ MISSING")
    print("----------------------------------------------")
    print(f"Resolved Host={cfg['host'] or '❌ MISSING'} Port={cfg['port']}")
    print("================================================\n")

    # env validation first
    if not cfg["host"] or not cfg["user"] or not cfg["database"] or not cfg["password"]:
        title, fix = explain_mysql_error(Exception("Missing env vars"), cfg, tcp_ok=False)
        print("❌ FLAG:", title)
        print("✅ HOW TO FIX:\n", fix)
        return False

    print("📡 Flag: Checking TCP connectivity to DB...")
    tcp_ok, tcp_msg = _tcp_check(cfg["host"], cfg["port"], timeout=5)
    print(("✅" if tcp_ok else "❌"), "TCP CHECK:", tcp_msg)

    print("📡 Flag: Connecting to Database (MySQL)...")
    try:
        db_pool = pooling.MySQLConnectionPool(
            pool_name="screenshot_pool",
            pool_size=MAX_THREADS + 2,
            **cfg
        )

        # quick query test
        c = db_pool.get_connection()
        cur = c.cursor()
        cur.execute("SELECT 1")
        cur.fetchall()
        cur.close()
        c.close()

        print("✅ FLAG: DATABASE CONNECTION SUCCESSFUL\n")
        return True

    except Exception as e:
        title, fix = explain_mysql_error(e, cfg, tcp_ok=tcp_ok)
        print("❌ FLAG:", title)
        print("✅ HOW TO FIX:\n", fix)
        print("🧾 DEBUG (masked):")
        print("    host:", cfg["host"], "port:", cfg["port"])
        print("    user:", _mask(cfg["user"], 3), "db:", _mask(cfg["database"], 3), "pass:", ("✅ SET" if cfg["password"] else "❌"))
        print("    raw error:", str(e))
        print()
        return False

# ---------------- HELPERS ---------------- #

def save_to_mysql(symbol, timeframe, image_data, chart_date, month_val):
    if db_pool is None:
        return False
    try:
        conn = db_pool.get_connection()
        cursor = conn.cursor()
        query = """
            INSERT INTO another_screenshot (symbol, timeframe, screenshot, chart_date, month_before) 
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
                screenshot = VALUES(screenshot),
                chart_date = VALUES(chart_date),
                month_before = VALUES(month_before),
                created_at = CURRENT_TIMESTAMP
        """
        cursor.execute(query, (symbol, timeframe, image_data, chart_date, month_val))
        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception as err:
        print(f"    ❌ DB SAVE ERROR [{symbol}]: {err}")
        return False

def get_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    return webdriver.Chrome(options=opts)

def force_clear_ads(driver):
    try:
        driver.execute_script("""
            const ads = [
                "div[class*='overlap-manager']", 
                "div[class*='dialog-']", 
                "div[class*='popup-']",
                "div[class*='drawer-']",
                "div[id*='overlap-manager']",
                "[data-role='toast-container']",
                "button[aria-label='Close']",
                "div[class*='notification-']"
            ];
            ads.forEach(selector => {
                document.querySelectorAll(selector).forEach(el => el.remove());
            });
        """)
    except:
        pass

def process_row(row):
    global processed_count
    row_clean = {str(k).lower().strip(): v for k, v in row.items()}
    symbol = str(row_clean.get('symbol', '')).strip()
    day_url = str(row_clean.get('day', '')).strip()
    target_date = str(row_clean.get('dates', '')).strip()

    with progress_lock:
        processed_count += 1
        current_idx = processed_count

    if not symbol or "tradingview.com" not in day_url:
        return

    print(f"🚀 [{current_idx}/{total_rows}] Flag: Capturing {symbol}...")

    driver = get_driver()
    try:
        driver.get("https://www.tradingview.com/")
        cookie_data = os.getenv("TRADINGVIEW_COOKIES")
        if cookie_data:
            for c in json.loads(cookie_data):
                driver.add_cookie({"name": c["name"], "value": c["value"], "domain": ".tradingview.com", "path": "/"})
            driver.refresh()

        driver.get(day_url)
        wait = WebDriverWait(driver, 30)
        chart = wait.until(EC.presence_of_element_located((By.XPATH, "//div[contains(@class, 'chart-container')]")))

        force_clear_ads(driver)

        ActionChains(driver).move_to_element(chart).click().perform()
        time.sleep(1)
        ActionChains(driver).key_down(Keys.ALT).send_keys('g').key_up(Keys.ALT).perform()

        input_xpath = "//input[contains(@class, 'query') or @data-role='search' or contains(@class, 'input')]"
        goto_input = wait.until(EC.visibility_of_element_located((By.XPATH, input_xpath)))
        goto_input.send_keys(Keys.CONTROL + "a" + Keys.BACKSPACE)
        goto_input.send_keys(target_date + Keys.ENTER)

        for _ in range(3):
            time.sleep(2)
            force_clear_ads(driver)

        driver.execute_script("document.querySelectorAll(\"div[class*='overlap-manager']\").forEach(e => e.remove());")

        img = chart.screenshot_as_png

        month_val = "Unknown"
        try:
            month_val = datetime.strptime(re.sub(r'[*]', '', target_date).strip(), "%Y-%m-%d").strftime('%B')
        except:
            pass

        if save_to_mysql(symbol, "day", img, target_date, month_val):
            print(f"✅ [{current_idx}/{total_rows}] FLAG: SUCCESS - {symbol}")
        else:
            print(f"❌ [{current_idx}/{total_rows}] FLAG: DB ERROR - {symbol}")

    except Exception as e:
        print(f"⚠️ [{current_idx}/{total_rows}] FLAG: ERROR {symbol}: {str(e)[:120]}")
    finally:
        driver.quit()

# ---------------- MAIN ---------------- #

def main():
    global total_rows

    # 1) DB must be OK before doing anything else
    if not init_db_pool():
        print("🛑 Stopping because DB is not reachable. Fix DB/secrets first.\n")
        return

    # 2) Load Google sheet
    try:
        creds = json.loads(os.getenv("GSPREAD_CREDENTIALS"))
        gc = gspread.service_account_from_dict(creds)
        spreadsheet = gc.open(SPREADSHEET_NAME)
        worksheet = spreadsheet.worksheet(TAB_NAME)
        all_values = worksheet.get_all_values()

        headers = [h.strip() for h in all_values[0]]
        df = pd.DataFrame(all_values[1:], columns=headers)
        df = df.loc[:, ~df.columns.duplicated()].copy()
        rows = df.to_dict('records')
        total_rows = len(rows)
        print(f"✅ FLAG: LOADED {total_rows} SYMBOLS")
    except Exception as e:
        print(f"❌ FLAG: GOOGLE SHEETS ERROR: {e}")
        return

    # 3) Process rows
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        list(executor.map(process_row, rows))

    print("\n🏁 FLAG: COMPLETED.")

if __name__ == "__main__":
    main()
