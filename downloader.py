import time
import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DOWNLOAD_DIR = os.path.join(BASE_DIR, "downloads")

EMAIL = "superadmin@fantasyadda.com"
PASSWORD = "Aditya@2005"


def _csv_mtime_map():
    files = {}
    for name in os.listdir(DOWNLOAD_DIR):
        if not name.lower().endswith(".csv"):
            continue
        path = os.path.join(DOWNLOAD_DIR, name)
        try:
            files[path] = os.path.getmtime(path)
        except OSError:
            continue
    return files


def _wait_for_downloaded_csv(before_mtimes, timeout):
    start = time.time()
    while time.time() - start < timeout:
        now = _csv_mtime_map()

        # New file name
        for path in now:
            if path not in before_mtimes and not path.endswith(".crdownload"):
                return path

        # Same file name overwritten / updated
        for path, mtime in now.items():
            prev_mtime = before_mtimes.get(path)
            if prev_mtime is not None and mtime > prev_mtime and not path.endswith(".crdownload"):
                return path

        time.sleep(1)

    raise FileNotFoundError("CSV download timeout: no new/updated CSV found in downloads directory")


def _click_download_button(driver, wait):
    selectors = [
        (By.XPATH, "//button[contains(translate(., 'DOWNLOAD', 'download'), 'download')]"),
        (By.XPATH, "//a[contains(translate(., 'DOWNLOAD', 'download'), 'download')]"),
        (By.XPATH, "//*[contains(@class, 'download') and (self::button or self::a)]"),
    ]

    last_error = None
    for by, sel in selectors:
        try:
            btn = wait.until(EC.element_to_be_clickable((by, sel)))
            try:
                btn.click()
            except Exception:
                driver.execute_script("arguments[0].click();", btn)
            return
        except Exception as e:
            last_error = e
            continue

    raise TimeoutException(f"Download button not clickable. Last selector error: {last_error}")


def download_withdraw_csv(timeout=60, max_attempts=2):
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    last_error = None

    for attempt in range(1, max_attempts + 1):
        before_mtimes = _csv_mtime_map()
        driver = None
        try:
            options = Options()

            # VPS required flags
            options.add_argument("--headless=new")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-gpu")
            options.add_argument("--window-size=1920,1080")

            prefs = {
                "download.default_directory": DOWNLOAD_DIR,
                "download.prompt_for_download": False,
                "download.directory_upgrade": True,
                "safebrowsing.enabled": True,
            }
            options.add_experimental_option("prefs", prefs)

            driver = webdriver.Chrome(
                service=Service(ChromeDriverManager().install()),
                options=options
            )

            # Required for headless download
            driver.execute_cdp_cmd(
                "Page.setDownloadBehavior",
                {
                    "behavior": "allow",
                    "downloadPath": DOWNLOAD_DIR
                }
            )

            print(f"🌐 Opening admin panel (attempt {attempt}/{max_attempts})")
            driver.get("https://admin.fantasyadda.com/admin")

            wait = WebDriverWait(driver, 30)
            wait.until(EC.presence_of_element_located((By.NAME, "email"))).send_keys(EMAIL)
            driver.find_element(By.NAME, "password").send_keys(PASSWORD)
            driver.find_element(By.TAG_NAME, "button").click()
            wait.until(EC.url_contains("admin"))
            print("✅ Logged in")

            driver.get("https://admin.fantasyadda.com/admin/registerusers/manual-withdraw-amount-bank")
            _click_download_button(driver, wait)
            print("⬇️ Download triggered")

            csv_path = _wait_for_downloaded_csv(before_mtimes, timeout)
            print("✅ CSV downloaded:", csv_path)
            return csv_path

        except (TimeoutException, FileNotFoundError, WebDriverException, OSError) as e:
            last_error = e
            print(f"⚠️ Download attempt {attempt} failed: {e}")
            if attempt == max_attempts:
                break
            time.sleep(2)
        finally:
            if driver is not None:
                driver.quit()
                print("🛑 Chrome closed")

    raise RuntimeError(f"CSV download failed after {max_attempts} attempts: {last_error}")
