# from playwright.sync_api import sync_playwright

# def capture_x_mas(url="https://www.fotmob.com/", trigger_js=None):
#     """
#     Navigate to `url`. Optionally run trigger_js (a JS string) to cause the site to compute/send the header.
#     Returns the first X-MAS value seen or None.
#     """
#     with sync_playwright() as p:
#         browser = p.chromium.launch(headless=True, args=['--no-sandbox'])
#         context = browser.new_context()
#         page = context.new_page()

#         found = {"value": None}
#         def on_request(request):
#             headers = {k.lower(): v for k, v in request.headers.items()}
#             if "x-mas" in headers:
#                 found["value"] = headers["x-mas"]

#         page.on("request", on_request)

#         page.goto(url, wait_until="load", timeout=10000)#, wait_until="networkidle"
#         if trigger_js:
#             page.evaluate(trigger_js)
#         page.wait_for_timeout(5000)

#         browser.close()
#         return found["value"]

from seleniumwire import webdriver
from selenium.webdriver.chrome.options import Options

def capture_x_mas(url="https://www.fotmob.com/", trigger_js=None):
    opts = Options()
    opts.add_argument("--headless=new")
    driver = webdriver.Chrome(options=opts)
    driver.get("https://www.fotmob.com/")

    # Give time for requests to load
    import time; time.sleep(3)

    x_mas = None
    # Print all requests and headers to debug
    for req in driver.requests:
        if req.response:
            # print(f"Request URL: {req.url}")
            # print(f"Request Headers: {req.headers}")
            if 'X-MAS' in req.headers:
                x_mas = req.headers.get('X-MAS') or req.headers.get('x-mas')
                # print(f"Found X-MAS header: {x_mas}")
                break

    driver.quit()
    # print("X_MAS:", x_mas)
    return x_mas

if __name__ == "__main__":
    val = capture_x_mas("https://www.fotmob.com/")
    print("X_MAS:", val)