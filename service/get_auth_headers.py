import logging
import time

from seleniumwire import webdriver
from selenium.webdriver.chrome.options import Options


def _suppress_selenium_wire_logs():
    """
    Suppress verbose selenium-wire logs.
    
    These loggers spam the console with every HTTP request/response
    made by the browser (CSS, JS, images, ads, etc.)
    """
    # Suppress selenium-wire's internal loggers
    logging.getLogger('seleniumwire.handler').setLevel(logging.WARNING)
    logging.getLogger('seleniumwire.server').setLevel(logging.WARNING)
    logging.getLogger('seleniumwire.backend').setLevel(logging.WARNING)
    logging.getLogger('seleniumwire.storage').setLevel(logging.WARNING)
    logging.getLogger('seleniumwire').setLevel(logging.WARNING)
    
    # Also suppress urllib3 and hpack (used by selenium-wire)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('hpack').setLevel(logging.WARNING)


def capture_x_mas(url="https://www.fotmob.com/", trigger_js=None):
    """
    Capture X-MAS authentication token from FotMob using Selenium.
    
    Args:
        url: FotMob URL to visit
        trigger_js: Optional JavaScript to trigger (not used currently)
        
    Returns:
        X-MAS token string or None if not captured
    """
    # Suppress verbose logging before initializing driver
    _suppress_selenium_wire_logs()
    
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    
    # Selenium-wire options to reduce logging
    seleniumwire_options = {
        'suppress_connection_errors': True,
        'verify_ssl': False,
    }
    
    driver = webdriver.Chrome(
        options=opts,
        seleniumwire_options=seleniumwire_options
    )
    
    try:
        driver.get(url)
        
        # Give time for requests to load
        time.sleep(3)
        
        x_mas = None
        for req in driver.requests:
            if req.response:
                if 'X-MAS' in req.headers:
                    x_mas = req.headers.get('X-MAS') or req.headers.get('x-mas')
                    break
        
        return x_mas
        
    finally:
        driver.quit()

if __name__ == "__main__":
    token = capture_x_mas()
    if token:
        print(f"✅ X-MAS token captured: {token[:50]}...")
    else:
        print("❌ Failed to capture X-MAS token")