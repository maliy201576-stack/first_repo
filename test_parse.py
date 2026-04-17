"""Quick test script to check actual HTML structure of FL.ru and Habr Freelance."""
import asyncio
from playwright.async_api import async_playwright

async def main():
    pw = await async_playwright().start()
    browser = await pw.chromium.launch(headless=True)

    # FL.ru
    page = await browser.new_page()
    await page.goto("https://www.fl.ru/projects/", wait_until="domcontentloaded", timeout=30000)
    html = await page.content()
    with open("/tmp/fl_ru.html", "w", encoding="utf-8") as f:
        f.write(html)
    print(f"FL.ru HTML saved: {len(html)} chars")
    # Check what project containers exist
    items = await page.query_selector_all("div.b-post")
    print(f"FL.ru div.b-post count: {len(items)}")
    # Try broader selectors
    for sel in ["div[class*=post]", "div[class*=project]", "article", "div[class*=card]", "a[href*='/projects/']"]:
        items = await page.query_selector_all(sel)
        print(f"  FL.ru '{sel}': {len(items)}")
    await page.close()

    # Habr Freelance
    page2 = await browser.new_page()
    await page2.goto("https://freelance.habr.com/tasks", wait_until="domcontentloaded", timeout=30000)
    html2 = await page2.content()
    with open("/tmp/habr.html", "w", encoding="utf-8") as f:
        f.write(html2)
    print(f"Habr HTML saved: {len(html2)} chars")
    items2 = await page2.query_selector_all("li.content-list__item")
    print(f"Habr li.content-list__item count: {len(items2)}")
    for sel in ["div[class*=task]", "article", "li[class*=item]", "a[href*='/task/']"]:
        items2 = await page2.query_selector_all(sel)
        print(f"  Habr '{sel}': {len(items2)}")
    await page2.close()

    await browser.close()
    await pw.stop()

asyncio.run(main())
