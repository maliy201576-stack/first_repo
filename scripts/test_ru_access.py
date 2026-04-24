"""Quick local test: can we reach profi.ru and zakupki.gov.ru with Playwright?

Run:
    python scripts/test_ru_access.py
"""

from __future__ import annotations

import asyncio
import time


_TARGETS = [
    ("profi.ru", "https://profi.ru/it_freelance/programmer/"),
    ("zakupki.gov.ru", "https://zakupki.gov.ru/epz/order/extendedsearch/results.html?searchString=&morphology=on"),
]


async def main() -> None:
    from playwright.async_api import async_playwright

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)

        for name, url in _TARGETS:
            print(f"\n{'='*60}")
            print(f"Testing: {name}")
            print(f"URL: {url}")
            print(f"{'='*60}")

            context = await browser.new_context()
            page = await context.new_page()

            start = time.monotonic()
            try:
                resp = await page.goto(url, wait_until="domcontentloaded", timeout=60_000)
                elapsed = time.monotonic() - start
                status = resp.status if resp else "no response"
                title = await page.title()
                body_len = len(await page.content())
                print(f"  Status:  {status}")
                print(f"  Title:   {title[:80]}")
                print(f"  HTML:    {body_len:,} chars")
                print(f"  Time:    {elapsed:.1f}s")
                print(f"  Result:  OK")
            except Exception as exc:
                elapsed = time.monotonic() - start
                print(f"  Error:   {type(exc).__name__}: {exc}")
                print(f"  Time:    {elapsed:.1f}s")
                print(f"  Result:  FAILED")
            finally:
                await context.close()

        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
