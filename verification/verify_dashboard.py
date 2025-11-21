from playwright.sync_api import sync_playwright

def verify_dashboard(page):
    # Go to the dashboard page
    # Note: In a real scenario, we would need to login first.
    # But here we are just verifying if the page renders (or redirects to login).
    # If we can't login easily, we might just check the HTML file, but let's try to visit.
    # Since the previous verification showed connection refused, we need to ensure app is running.

    # Assuming the app is running (I'll check/start it in the plan).
    page.goto("http://localhost:5000/dashboard")

    # Even if it redirects to login, we can't see the dashboard content without credentials.
    # However, the user can verify it visually.
    # I'll just take a screenshot of whatever page we land on.
    # If it's the login page, I can't verify the dashboard change via screenshot automatically without credentials.

    page.screenshot(path="/home/jules/verification/dashboard_check.png")
    print("Screenshot taken")

if __name__ == "__main__":
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        try:
            verify_dashboard(page)
        except Exception as e:
            print(f"Error: {e}")
        finally:
            browser.close()
