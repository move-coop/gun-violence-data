from selenium import webdriver
import time
import os


def setup_selenium(download_path=None):
    """
    Sets up a headless webdriver for using Selenium in a Civis container
    according to how its configured in the parsons docker image.

    Call this at the beginning of your script.

    `Args:`
        download_path: str
            A path indicating where files will be downloaded. For example:
            '/app/vendors/bloomerang'. The path begins with '/app/' in order to
            run in a Civis container.
    """

    if download_path is None:
        download_path = os.getcwd()

    # Chrome webdriver setup
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--disable-gpu')

    # change download directory to PATH
    chrome_prefs = {"download.default_directory": download_path}
    chrome_options.add_experimental_option("prefs", chrome_prefs)

    driver = webdriver.Chrome(options=chrome_options)

    driver.command_executor._commands["send_command"] = (
        "POST", '/session/$sessionId/chromium/send_command')

    # allow for direct download
    params = {'cmd': 'Page.setDownloadBehavior', 'params': {
        'behavior': 'allow', 'downloadPath': download_path}
        }

    driver.execute("send_command", params)
    # some time for setup up to finish
    time.sleep(20)

    return driver
