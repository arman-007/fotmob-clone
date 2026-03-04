"""
Get Auth Headers Module

Captures X-MAS authentication token from FotMob using Selenium with Firefox.
Optimized for low-memory server environments with retry logic.
"""

import logging
import time

from seleniumwire import webdriver
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.firefox.service import Service as FirefoxService
from selenium.common.exceptions import TimeoutException, WebDriverException

from utils.logging_config import suppress_noisy_loggers

logger = logging.getLogger(__name__)


def _create_firefox_driver(seleniumwire_options: dict):
    """
    Create and configure Firefox WebDriver instance.
    
    Returns:
        Configured Firefox WebDriver
    """
    opts = FirefoxOptions()
    opts.add_argument("--headless")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--width=1920")
    opts.add_argument("--height=1080")
    
    # Use 'eager' page load strategy - don't wait for all resources
    # This helps avoid timeout when page has slow-loading elements
    opts.page_load_strategy = 'eager'
    
    # Memory optimization preferences
    opts.set_preference("browser.cache.disk.enable", False)
    opts.set_preference("browser.cache.memory.enable", False)
    opts.set_preference("browser.cache.offline.enable", False)
    opts.set_preference("network.http.use-cache", False)
    opts.set_preference("browser.sessionhistory.max_entries", 2)
    opts.set_preference("browser.sessionhistory.max_total_viewers", 0)
    
    # Disable unnecessary features to save memory and speed up
    opts.set_preference("media.autoplay.default", 5)  # Block autoplay
    opts.set_preference("media.video_stats.enabled", False)
    opts.set_preference("dom.webnotifications.enabled", False)
    opts.set_preference("geo.enabled", False)
    opts.set_preference("dom.push.enabled", False)
    opts.set_preference("toolkit.telemetry.enabled", False)
    opts.set_preference("datareporting.healthreport.uploadEnabled", False)
    
    # Disable images to reduce memory and speed up loading
    opts.set_preference("permissions.default.image", 2)
    
    # Network optimization
    opts.set_preference("network.http.pipelining", True)
    opts.set_preference("network.http.proxy.pipelining", True)
    opts.set_preference("network.http.max-connections", 48)
    opts.set_preference("network.http.max-connections-per-server", 16)
    
    # Disable animations and smooth scrolling
    opts.set_preference("toolkit.cosmeticAnimations.enabled", False)
    opts.set_preference("general.smoothScroll", False)
    
    driver = webdriver.Firefox(
        options=opts,
        seleniumwire_options=seleniumwire_options
    )
    
    # Set timeouts
    driver.set_page_load_timeout(60)  # Increased to 60 seconds
    driver.set_script_timeout(30)
    driver.implicitly_wait(10)
    
    return driver


def _extract_x_mas_from_requests(driver) -> str | None:
    """
    Extract X-MAS token from captured requests.
    
    Args:
        driver: Selenium WebDriver with captured requests
        
    Returns:
        X-MAS token string or None
    """
    for req in driver.requests:
        if req.response:
            x_mas = req.headers.get('X-MAS') or req.headers.get('x-mas')
            if x_mas:
                logger.debug(f"X-MAS token found in request to: {req.url[:50]}...")
                return x_mas
    return None


def capture_x_mas(url="https://www.fotmob.com/", max_retries=3, retry_delay=5):
    """
    Capture X-MAS authentication token from FotMob using Selenium with Firefox.
    
    Optimized for low-memory servers (2GB RAM) with retry logic.
    
    Args:
        url: FotMob URL to visit
        max_retries: Maximum number of retry attempts (default: 3)
        retry_delay: Seconds to wait between retries (default: 5)
        
    Returns:
        X-MAS token string or None if not captured
    """
    # Suppress verbose logging before initializing driver
    suppress_noisy_loggers()
    
    # Selenium-wire options
    seleniumwire_options = {
        'suppress_connection_errors': True,
        'verify_ssl': False,
    }
    
    last_error = None
    
    for attempt in range(1, max_retries + 1):
        driver = None
        try:
            logger.debug(f"X-MAS capture attempt {attempt}/{max_retries}")
            
            driver = _create_firefox_driver(seleniumwire_options)
            
            # Clear any existing requests
            del driver.requests
            
            # Navigate to FotMob
            try:
                driver.get(url)
            except TimeoutException:
                # Even if page load times out, we might have captured the token
                logger.warning(f"Page load timed out on attempt {attempt}, checking captured requests...")
            
            # Give time for API requests to be made
            time.sleep(3)
            
            # Try to extract token from captured requests
            x_mas = _extract_x_mas_from_requests(driver)
            
            if x_mas:
                logger.info(f"✅ X-MAS token captured successfully on attempt {attempt}")
                return x_mas
            
            logger.warning(f"No X-MAS token found in requests on attempt {attempt}")
            
        except WebDriverException as e:
            last_error = e
            logger.warning(f"WebDriver error on attempt {attempt}: {e}")
            
        except Exception as e:
            last_error = e
            logger.error(f"Error capturing X-MAS token on attempt {attempt}: {e}")
            
        finally:
            if driver:
                try:
                    driver.quit()
                except Exception as e:
                    logger.debug(f"Error closing driver: {e}")
        
        # Wait before retry (except on last attempt)
        if attempt < max_retries:
            logger.info(f"Waiting {retry_delay}s before retry...")
            time.sleep(retry_delay)
    
    logger.error(f"❌ Failed to capture X-MAS token after {max_retries} attempts. Last error: {last_error}")
    return None


if __name__ == "__main__":
    # Configure logging for standalone testing
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    print("Testing X-MAS token capture with Firefox...")
    print("This may take up to 60 seconds per attempt (3 attempts max)...")
    
    token = capture_x_mas()
    
    if token:
        print(f"✅ X-MAS token captured: {token[:50]}...")
    else:
        print("❌ Failed to capture X-MAS token after all retries")