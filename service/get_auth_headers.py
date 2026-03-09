from service.playwright_auth import capture_auth_info_playwright
import logging

logger = logging.getLogger(__name__)

def capture_auth_info(url="https://www.fotmob.com/", max_retries=3, retry_delay=5):
    """
    Capture auth information (X-MAS token and cookies) using Playwright.
    Wrapper for capture_auth_info_playwright for backward compatibility.
    """
    return capture_auth_info_playwright(url, max_retries)

if __name__ == "__main__":
    # Test capture
    logging.basicConfig(level=logging.INFO)
    info = capture_auth_info()
    if info:
        print(f"✅ Captured auth info:")
        print(f"X-MAS: {info['x_mas'][:50]}...")
        print(f"Cookies: {list(info['cookies'].keys())}")
    else:
        print("❌ Failed to capture auth info")