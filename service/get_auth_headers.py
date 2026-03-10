import logging

logger = logging.getLogger(__name__)

try:
    from service.playwright_auth import capture_auth_info_playwright
except ImportError:
    capture_auth_info_playwright = None


def capture_auth_info(url="https://www.fotmob.com/", max_retries=3, retry_delay=5, no_browser=False):
    """
    Capture auth information (X-MAS token and cookies) using Playwright.
    Wrapper for capture_auth_info_playwright for backward compatibility.

    If no_browser=True or Playwright is not installed, returns None immediately.
    The pipeline can still work using dynamically generated x-mas headers from auth_utils.
    """
    if no_browser:
        logger.info("no_browser mode: skipping browser-based auth capture")
        return None
    if capture_auth_info_playwright is None:
        logger.warning("Playwright not installed, skipping browser-based auth capture")
        return None
    return capture_auth_info_playwright(url, max_retries)


if __name__ == "__main__":
    # Test capture
    logging.basicConfig(level=logging.INFO)
    info = capture_auth_info()
    if info:
        print(f"Captured auth info:")
        print(f"X-MAS: {info['x_mas'][:50]}...")
        print(f"Cookies: {list(info['cookies'].keys())}")
    else:
        print("Failed to capture auth info")
