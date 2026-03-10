import time
import logging
import json
import base64
import random
from typing import Dict, Optional, Any

logger = logging.getLogger(__name__)

# Optional Playwright imports — pipeline works without them in --no-browser mode
try:
    from playwright.sync_api import sync_playwright
    from playwright_stealth import Stealth
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
    logger.info("Playwright not installed. Browser-based fallbacks will be unavailable.")

# Default to headless=True for server environments (no display required)
_HEADLESS_MODE = True

_BROWSER_ARGS = [
    '--no-sandbox',
    '--disable-blink-features=AutomationControlled',
    '--disable-dev-shm-usage',
]

_GLOBAL_PLAYWRIGHT = None
_GLOBAL_BROWSER = None
_GLOBAL_CONTEXT = None


def set_headless_mode(headless: bool):
    """Configure whether Playwright launches in headless mode (default: True)."""
    global _HEADLESS_MODE
    _HEADLESS_MODE = headless


def get_playwright_context():
    """Get or create a persistent Playwright context."""
    global _GLOBAL_PLAYWRIGHT, _GLOBAL_BROWSER, _GLOBAL_CONTEXT

    if not PLAYWRIGHT_AVAILABLE:
        raise RuntimeError("Playwright is not installed. Install with: pip install playwright playwright-stealth && playwright install chromium")

    if _GLOBAL_CONTEXT:
        try:
            if _GLOBAL_BROWSER.is_connected():
                return _GLOBAL_CONTEXT
        except:
            pass

    if _GLOBAL_PLAYWRIGHT:
        try:
            _GLOBAL_PLAYWRIGHT.stop()
        except:
            pass

    _GLOBAL_PLAYWRIGHT = sync_playwright().start()
    _GLOBAL_BROWSER = _GLOBAL_PLAYWRIGHT.chromium.launch(
        headless=_HEADLESS_MODE,
        args=_BROWSER_ARGS,
    )
    user_agent = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36'
    _GLOBAL_CONTEXT = _GLOBAL_BROWSER.new_context(
        user_agent=user_agent,
        viewport={'width': 1920, 'height': 1080}
    )
    return _GLOBAL_CONTEXT

def fetch_json_playwright(url: str, headers: Optional[Dict[str, str]] = None, max_retries: int = 2) -> Optional[Dict[str, Any]]:
    """Fetch JSON data using in-page fetch() to bypass Cloudflare."""
    if not PLAYWRIGHT_AVAILABLE:
        logger.warning("Playwright not available, cannot use browser fallback")
        return None
    context = get_playwright_context()
    stealth_util = Stealth()
    
    # Try to find a sensible "parent" page to navigate to first
    parent_url = "https://www.fotmob.com/"
    if "matchId=" in url:
        match_id = url.split("matchId=")[1].split("&")[0]
        parent_url = f"https://www.fotmob.com/match/{match_id}"
    
    for attempt in range(1, max_retries + 1):
        page = None
        try:
            page = context.new_page()
            stealth_util.apply_stealth_sync(page)
            
            logger.info(f"Playwright: Navigating to parent page {parent_url}...")
            page.goto(parent_url, wait_until="domcontentloaded", timeout=60000)
            
            # Wait a bit for challenges
            time.sleep(5)
            
            logger.info(f"Playwright: Fetching JSON from {url} via in-page fetch...")
            
            # Capture console logs from the page
            def handle_console(msg):
                logger.info(f"Playwright Console: {msg.text}")
            page.on("console", handle_console)
            
            # Use page.evaluate to call the native fetch()
            headers_json = json.dumps(headers or {})
            fetch_script = f"""
            async () => {{
                try {{
                    const customHeaders = {headers_json};
                    console.log('Starting in-page fetch to: {url}');
                    const response = await fetch('{url}', {{
                        headers: customHeaders
                    }});
                    console.log('Fetch response status: ' + response.status);
                    if (!response.ok) return {{ error: 'Status ' + response.status }};
                    const data = await response.json();
                    console.log('Successfully parsed JSON');
                    return data;
                }} catch (e) {{
                    console.error('In-page fetch error: ' + e.message);
                    return {{ error: 'JS Error: ' + e.message }};
                }}
            }}
            """
            data = page.evaluate(fetch_script)
            
            if isinstance(data, dict) and "error" in data:
                logger.warning(f"Playwright in-page fetch error: {data['error']} (Attempt {attempt})")
                if "403" in str(data['error']):
                    # Try home page solve
                    page.goto("https://www.fotmob.com/", wait_until="domcontentloaded", timeout=30000)
                    time.sleep(10)
                    continue
            else:
                return data
                
        except Exception as e:
            logger.error(f"Playwright: Error during in-page fetch iteration {attempt}: {e}")
        finally:
            if page:
                page.close()
                
    return None

def capture_auth_info_playwright(url: str = "https://www.fotmob.com/", max_retries: int = 3) -> Optional[Dict[str, Any]]:
    """
    Capture auth information (X-MAS token and cookies) using Playwright.
    More robust against Cloudflare detection.
    """
    if not PLAYWRIGHT_AVAILABLE:
        logger.warning("Playwright not available, cannot capture auth info via browser")
        return None
    stealth_util = Stealth()
    user_agent = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36'

    for attempt in range(1, max_retries + 1):
        try:
            with sync_playwright() as p:
                logger.info(f"Playwright: Starting capture attempt {attempt}...")
                browser = p.chromium.launch(
                    headless=_HEADLESS_MODE,
                    args=_BROWSER_ARGS,
                )
                
                context = browser.new_context(
                    user_agent=user_agent,
                    viewport={'width': 1920, 'height': 1080}
                )
                
                page = context.new_page()
                stealth_util.apply_stealth_sync(page)
                
                x_mas_token = None
                
                def handle_request(request):
                    nonlocal x_mas_token
                    if 'x-mas' in request.headers:
                        x_mas_token = request.headers['x-mas']

                page.on("request", handle_request)
                
                # Navigate to a match page to trigger Turnstile
                match_id = 4830678 
                target_url = f"https://www.fotmob.com/match/{match_id}"
                try:
                    logger.info(f"Playwright: Navigating to {target_url} to solve Turnstile...")
                    page.goto(target_url, wait_until="domcontentloaded", timeout=90000)
                except Exception as e:
                    logger.warning(f"Playwright: Navigation warning on attempt {attempt}: {e}")
                
                # Wait specifically for cf_clearance to appear (this is the magic cookie)
                start_time = time.time()
                while time.time() - start_time < 60:
                    # Filter cookies to only fotmob.com to avoid 400 Bad Request (oversized header)
                    cookies = context.cookies("https://www.fotmob.com")
                    cookie_dict = {c['name']: c['value'] for c in cookies}
                    
                    if x_mas_token and 'cf_clearance' in cookie_dict:
                        logger.info(f"✅ Playwright: Captured x-mas and cf_clearance on attempt {attempt}!")
                        browser.close()
                        return {
                            'x_mas': x_mas_token,
                            'cookies': cookie_dict,
                            'user_agent': user_agent
                        }
                    
                    # Mimic human activity while waiting
                    page.mouse.move(random.randint(0, 500), random.randint(0, 500))
                    time.sleep(3)
                
                # Final fallback for cookies
                cookies = context.cookies("https://www.fotmob.com")
                cookie_dict = {c['name']: c['value'] for c in cookies}
                
                if x_mas_token and len(cookie_dict) >= 3:
                    logger.warning(f"⚠️ Playwright: Captured {len(cookie_dict)} cookies for fotmob.com, but still missing cf_clearance.")
                    browser.close()
                    return {
                        'x_mas': x_mas_token,
                        'cookies': cookie_dict,
                        'user_agent': user_agent
                    }
                
                browser.close()
                logger.warning(f"Playwright: Incomplete info on attempt {attempt}")
                
        except Exception as e:
            logger.error(f"Playwright: Error on attempt {attempt}: {e}")
            
    return None

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    info = capture_auth_info_playwright()
    if info:
        print(f"Captured x-mas: {info['x_mas'][:50]}...")
        print(f"Cookies: {list(info['cookies'].keys())}")
        print(f"User Agent: {info['user_agent']}")
    else:
        print("Failed to capture.")
