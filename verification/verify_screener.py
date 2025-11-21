from playwright.sync_api import sync_playwright

def verify_screener(page):
    # Go to the screener page
    page.goto("http://localhost:5000/screener/")

    # Check if the page loaded and has the title/header
    # Note: The template extends base.html, so it should have some structure.
    # We look for the specific content we added.
    page.wait_for_selector("h1")

    # Take a screenshot
    page.screenshot(path="/home/jules/verification/screener_page.png")
    print("Screenshot taken")

if __name__ == "__main__":
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        try:
            verify_screener(page)
        except Exception as e:
            print(f"Error: {e}")
        finally:
            browser.close()
