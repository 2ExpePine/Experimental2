
import os
import re
import time
import shutil
import requests
import pymysql
import urllib.parse

from datetime import datetime
from contextlib import closing

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options

from selenium.webdriver.common.by import By

from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from webdriver_manager.chrome import ChromeDriverManager


# =========================================================
# DATABASE CONFIG
# =========================================================

DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_NAME'),
    'charset': 'utf8mb4'
}


# =========================================================
# DOWNLOAD DIRECTORY
# =========================================================

DOWNLOAD_DIR = os.path.join(os.getcwd(), "downloads")

if not os.path.exists(DOWNLOAD_DIR):
    os.makedirs(DOWNLOAD_DIR)


# =========================================================
# LOGGER
# =========================================================

def log(message):

    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] {message}"
    )


# =========================================================
# DATABASE CONNECTION
# =========================================================

def get_connection():

    return pymysql.connect(
        **DB_CONFIG,
        cursorclass=pymysql.cursors.DictCursor
    )


# =========================================================
# GET CHANNELS FROM DATABASE
# =========================================================

def get_channels():

    try:

        with closing(get_connection()) as conn:

            with conn.cursor() as cursor:

                sql = """
                SELECT *
                FROM youtube_channels
                WHERE active = 1
                ORDER BY id ASC
                """

                cursor.execute(sql)

                rows = cursor.fetchall()

                return rows

    except Exception as e:

        log(f"❌ Channel Fetch Error: {e}")

        return []


# =========================================================
# EXTRACT VIDEO ID
# =========================================================

def extract_video_id(url):

    parsed = urllib.parse.urlparse(url)

    if parsed.hostname == "youtu.be":
        return parsed.path[1:]

    if parsed.hostname and "youtube.com" in parsed.hostname:

        query = urllib.parse.parse_qs(parsed.query)

        return query.get("v", [None])[0]

    return None


# =========================================================
# GET LATEST VIDEOS
# =========================================================

def get_latest_videos(channel_url, limit=3):

    video_urls = []

    try:

        clean_url = channel_url.split('?')[0].rstrip('/')

        if not clean_url.endswith('/videos'):
            clean_url += '/videos'

        log(f"📺 Fetching videos from: {clean_url}")

        headers = {
            "User-Agent": "Mozilla/5.0"
        }

        response = requests.get(
            clean_url,
            headers=headers,
            timeout=30
        )

        html = response.text

        video_ids = re.findall(
            r'"videoId":"([^"]+)"',
            html
        )

        unique_ids = []

        for vid in video_ids:

            if vid not in unique_ids:

                unique_ids.append(vid)

        for vid in unique_ids[:limit]:

            video_urls.append(
                f"https://www.youtube.com/watch?v={vid}"
            )

        log(f"✅ Found {len(video_urls)} videos")

    except Exception as e:

        log(f"❌ Video Fetch Error: {e}")

    return video_urls


# =========================================================
# CREATE SELENIUM DRIVER
# =========================================================

def create_driver():

    options = Options()

    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    options.add_argument("--window-size=1920,1080")

    options.add_argument(
        "--disable-blink-features=AutomationControlled"
    )

    options.add_argument(
        "user-agent=Mozilla/5.0"
    )

    prefs = {
        "download.default_directory": DOWNLOAD_DIR,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    }

    options.add_experimental_option(
        "prefs",
        prefs
    )

    driver = webdriver.Chrome(
        service=Service(
            ChromeDriverManager().install()
        ),
        options=options
    )

    driver.execute_cdp_cmd(
        "Page.setDownloadBehavior",
        {
            "behavior": "allow",
            "downloadPath": DOWNLOAD_DIR
        }
    )

    return driver


# =========================================================
# CLEAN DOWNLOAD DIRECTORY
# =========================================================

def clean_downloads():

    try:

        for filename in os.listdir(DOWNLOAD_DIR):

            path = os.path.join(
                DOWNLOAD_DIR,
                filename
            )

            try:

                if os.path.isfile(path):
                    os.remove(path)

                elif os.path.isdir(path):
                    shutil.rmtree(path)

            except:
                pass

    except:
        pass


# =========================================================
# GET VIDEO TITLE + TRANSCRIPT
# =========================================================

def get_video_data(video_url):

    driver = create_driver()

    title = "Unknown Title"

    transcript = None

    try:

        downsub_url = (
            "https://downsub.com/?url="
            + urllib.parse.quote(video_url)
        )

        driver.get(downsub_url)

        wait = WebDriverWait(driver, 30)

        try:

            title_element = wait.until(

                EC.visibility_of_element_located(
                    (By.CSS_SELECTOR, ".card-header b")
                )

            )

            title = (
                title_element.text
                .replace("Download Subtitles", "")
                .strip()
            )

            if not title or len(title) < 3:

                title = (
                    driver.title
                    .split("-")[0]
                    .strip()
                )

        except:

            title = driver.title

        clean_downloads()

        txt_xpath = (
            "//button[contains(., 'TXT') "
            "or contains(., '[TXT]')]"
        )

        txt_button = wait.until(

            EC.element_to_be_clickable(
                (By.XPATH, txt_xpath)
            )

        )

        driver.execute_script(
            "arguments[0].click();",
            txt_button
        )

        timeout = 30

        start = time.time()

        while time.time() - start < timeout:

            files = [

                f for f in os.listdir(DOWNLOAD_DIR)

                if f.endswith(".txt")

            ]

            if files:

                file_path = os.path.join(
                    DOWNLOAD_DIR,
                    files[0]
                )

                time.sleep(1)

                with open(
                    file_path,
                    "r",
                    encoding="utf-8"
                ) as f:

                    transcript = f.read()

                break

            time.sleep(2)

        return title, transcript

    except Exception as e:

        log(f"❌ Transcript Error: {e}")

        return title, None

    finally:

        driver.quit()


# =========================================================
# SAVE TRANSCRIPT
# =========================================================

def save_transcript(

    channel_id,
    channel_name,
    video_id,
    video_url,
    title,
    transcript

):

    if not transcript:
        return

    try:

        with closing(get_connection()) as conn:

            with conn.cursor() as cursor:

                sql = """
                INSERT INTO all_transcripts
                (

                    channel_id,
                    channel_name,
                    video_id,
                    video_url,
                    title,
                    transcript

                )

                VALUES
                (%s,%s,%s,%s,%s,%s)

                ON DUPLICATE KEY UPDATE

                    title = VALUES(title),
                    transcript = VALUES(transcript),
                    updated_at = CURRENT_TIMESTAMP
                """

                cursor.execute(

                    sql,

                    (
                        channel_id,
                        channel_name,
                        video_id,
                        video_url,
                        title,
                        transcript
                    )

                )

            conn.commit()

        log(f"✅ Saved: {title[:70]}")

    except Exception as e:

        log(f"❌ Database Save Error: {e}")


# =========================================================
# MAIN
# =========================================================

if __name__ == "__main__":

    log("🚀 Starting Multi-Channel Transcript Scraper")

    channels = get_channels()

    if not channels:

        log("❌ No active channels found in database")

        exit()

    log(f"✅ Total Active Channels: {len(channels)}")

    for channel in channels:

        try:

            channel_id = channel['id']

            channel_name = channel['channel_name']

            channel_url = channel['channel_url']

            limit = channel['latest_video_limit']

            log("\n===================================")
            log(f"📡 CHANNEL: {channel_name}")
            log(f"🌐 URL: {channel_url}")
            log("===================================")

            videos = get_latest_videos(
                channel_url,
                limit
            )

            if not videos:

                log("❌ No videos found")

                continue

            for video_url in videos:

                try:

                    log(f"\n🎬 Processing Video")
                    log(video_url)

                    video_id = extract_video_id(
                        video_url
                    )

                    if not video_id:

                        log("❌ Invalid video ID")

                        continue

                    title, transcript = get_video_data(
                        video_url
                    )

                    if transcript:

                        save_transcript(

                            channel_id,
                            channel_name,
                            video_id,
                            video_url,
                            title,
                            transcript

                        )

                    else:

                        log("❌ Empty transcript")

                    time.sleep(2)

                except Exception as e:

                    log(f"❌ Video Error: {e}")

        except Exception as e:

            log(f"❌ Channel Error: {e}")

    log("\n✅ ALL CHANNELS COMPLETED")

