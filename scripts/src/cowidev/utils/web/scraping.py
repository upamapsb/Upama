from urllib.error import HTTPError

from bs4 import BeautifulSoup
import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options as ChroOpt
from selenium.webdriver.firefox.options import Options as FireOpt


def get_headers() -> dict:
    """Get generic header for requests.

    Returns:
        dict: Header.
    """
    return {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.16; rv:86.0) Gecko/20100101 Firefox/86.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Encoding": "*",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Pragma": "no-cache",
        "Cache-Control": "no-cache",
    }


def get_soup(
    source: str,
    headers: dict = None,
    verify: bool = True,
    from_encoding: str = None,
    timeout=20,
) -> BeautifulSoup:
    """Get soup from website.

    Args:
        source (str): Website url.
        headers (dict, optional): Headers to be used for request. Defaults to general one.
        verify (bool, optional): Verify source URL. Defaults to True.
        from_encoding (str, optional): Encoding to use. Defaults to None.
        timeout (int, optional): If no response is received after `timeout` seconds, exception is raied.
                                 Defaults to 20.
    Returns:
        BeautifulSoup: Website soup.
    """
    if headers is None:
        headers = get_headers()
    try:
        response = requests.get(source, headers=headers, verify=verify, timeout=timeout)
    except Exception as err:
        raise err
    if not response.ok:
        raise HTTPError("Web {} not found! {response.content}")
    content = response.content
    return BeautifulSoup(content, "html.parser", from_encoding=from_encoding)


def sel_options(headless: bool = True, firefox: bool = False):
    if firefox:
        op = FireOpt()
    else:
        op = ChroOpt()
        op.add_experimental_option(
            "prefs",
            {
                "download.prompt_for_download": False,
                "download.directory_upgrade": True,
                "safebrowsing.enabled": True,
            },
        )
    op.add_argument("--disable-notifications")
    if headless:
        op.add_argument("--headless")
    return op


def get_driver(headless: bool = True, download_folder: str = None, options=None, firefox: bool = False):
    if options is None:
        options = sel_options(headless=headless, firefox=firefox)
    if firefox:
        driver = webdriver.Firefox(options=options)
    else:
        driver = webdriver.Chrome(options=options)
    if download_folder:
        set_download_settings(driver, download_folder)
    return driver


def set_download_settings(driver, folder_name: str = None):
    if folder_name is None:
        folder_name = "/tmp"
    driver.command_executor._commands["send_command"] = (
        "POST",
        "/session/$sessionId/chromium/send_command",
    )
    params = {
        "cmd": "Page.setDownloadBehavior",
        "params": {"behavior": "allow", "downloadPath": folder_name},
    }
    _ = driver.execute("send_command", params)


def scroll_till_element(driver, element):
    desired_y = (element.size["height"] / 2) + element.location["y"]
    current_y = (driver.execute_script("return window.innerHeight") / 2) + driver.execute_script(
        "return window.pageYOffset"
    )
    scroll_y_by = desired_y - current_y
    driver.execute_script("window.scrollBy(0, arguments[0]);", scroll_y_by)
