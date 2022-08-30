from selenium import webdriver
from selenium.webdriver.chrome.service import Service
import time
import os
from webdriver_manager.chrome import ChromeDriverManager
import zipfile
import glob
import os

dir_path = os.path.dirname(os.path.realpath(__file__))


def get_latest_donations():
    url = "https://public.netfile.com/pub2/Default.aspx?aid=CRUZ"

    options = webdriver.ChromeOptions()
    prefs = {"download.default_directory": dir_path + "/downloads"}
    options.headless = True
    options.add_experimental_option("prefs", prefs)

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()), options=options
    )

    try:
        driver.get(url)
        # get the download link
        download = driver.find_element("id", "ctl00_phBody_GetExcelAmend")
        # initiate download
        download.click()
        # allow ample time for download
        time.sleep(10)
        # get
        driver.close()
    except Exception as err:
        print(err)

    # unzip files
    zipfiles = glob.glob(dir_path + "/downloads/*.zip")
    for path in zipfiles:
        with zipfile.ZipFile(path, "r") as zip_ref:
            zip_ref.extractall(dir_path + "./data")
