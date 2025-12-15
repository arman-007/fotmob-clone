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
    for req in driver.requests:
        if req.response:
            if 'X-MAS' in req.headers:
                x_mas = req.headers.get('X-MAS') or req.headers.get('x-mas')
                break

    driver.quit()
    return x_mas

if __name__ == "__main__":
    val = capture_x_mas("https://www.fotmob.com/")
    print("X_MAS:", val)