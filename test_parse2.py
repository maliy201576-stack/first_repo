"""Check inner HTML structure of FL.ru project items."""
import asyncio
from playwright.async_api import async_playwright

async def main():
    pw = await async_playwright().start()
    browser = await pw.chromium.launch(headless=True)
    page = await browser.new_page()
    await page.goto("https://www.fl.ru/projects/", wait_until="domcontentloaded", timeout=30000)

    # Get first b-post inner HTML
    item = await page.query_selector("div.b-post")
    if item:
        inner = await item.inner_html()
        print("FIRST div.b-post inner HTML (first 2000 chars):")
        print(inner[:2000])
        print("---")
        # Check for links
        link = await item.query_selector("a.b-post__link")
        print(f"a.b-post__link: {link}")
        links = await item.query_selector_all("a")
        for a in links[:5]:
            href = await a.get_attribute("href")
            text = await a.inner_text()
            cls = await a.get_attribute("class")
            print(f"  <a class='{cls}' href='{href}'>{text[:80]}")

    # Habr - check page title and body snippet
    page2 = await browser.new_page()
    await page2.goto("https://freelance.habr.com/tasks", wait_until="domcontentloaded", timeout=30000)
    title = await page2.title()
    print(f"\nHabr page title: {title}")
    body = await page2.inner_text("body")
    print(f"Habr body text (first 500 chars): {body[:500]}")

    await browser.close()
    await pw.stop()

asyncio.run(main())
