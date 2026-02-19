import time
import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DOWNLOAD_DIR = os.path.join(BASE_DIR, "downloads")

EMAIL = "superadmin@fantasyadda.com"
PASSWORD = "Aditya@2005"

# ✅ macOS Chrome binary path
CHROME_BINARY = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"


def download_withdraw_csv(timeout=40):
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    before_files = set(os.listdir(DOWNLOAD_DIR))

    chrome_options = Options()
    chrome_options.binary_location = CHROME_BINARY

    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")

    chrome_options.add_experimental_option(
        "prefs",
        {
            "download.default_directory": DOWNLOAD_DIR,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True,
        },
    )

    # ✅ webdriver-manager handles correct chromedriver
    service = Service(ChromeDriverManager().install())

    driver = webdriver.Chrome(
        service=service,
        options=chrome_options
    )

    try:
        print("🌐 Opening admin panel")

        driver.get("https://admin.fantasyadda.com/admin")
        time.sleep(3)

        driver.find_element(By.NAME, "email").send_keys(EMAIL)
        driver.find_element(By.NAME, "password").send_keys(PASSWORD)
        driver.find_element(By.TAG_NAME, "button").click()

        time.sleep(5)

        print("✅ Logged in successfully")

        driver.get(
            "https://admin.fantasyadda.com/admin/registerusers/manual-withdraw-amount-bank"
        )
        time.sleep(5)

        print("⬇️ Clicking download button")
        wait = WebDriverWait(driver, 30)

        download_btn = wait.until(
            EC.element_to_be_clickable((
                By.XPATH,
                "//button[contains(translate(., 'DOWNLOAD', 'download'), 'download')]"
            ))
        )
        download_btn.click()

        start = time.time()
        while time.time() - start < timeout:
            after_files = set(os.listdir(DOWNLOAD_DIR))
            new_files = after_files - before_files

            for f in new_files:
                if f.endswith(".csv") and not f.endswith(".crdownload"):
                    csv_path = os.path.join(DOWNLOAD_DIR, f)
                    print("✅ CSV downloaded:", csv_path)
                    return csv_path

            time.sleep(1)

        raise FileNotFoundError("CSV download timeout")

    finally:
        driver.quit()
        print("🛑 Chrome closed")
