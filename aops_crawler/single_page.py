from __future__ import annotations
from scrapy.http import HtmlResponse, Response,JsonResponse
import asyncio
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs
import json
from pathlib import Path
import time

from patchright.async_api import TimeoutError as PlaywrightTimeoutError
from patchright.async_api import async_playwright

 
async def create_storage_state_interactive(
    *,
    storage_state_path: str = "state.json",
    start_url: str = "https://artofproblemsolving.com/",
    headless: bool = False,
    browser_channel: Optional[str] = "msedge",
) -> str:
    """
    Open a real browser so you can log in manually, then save the session to a storage state file.

    Returns the path to the saved storage state JSON.
    """
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless, channel=browser_channel)
        context = await browser.new_context()
        page = await context.new_page()

        # Track last successful XHR/fetch completion to decide when to stop
        last_network_ts = time.monotonic()
        capture_types = {"xhr", "fetch"}

        async def _on_request_finished(req):  # type: ignore[no-redef]
            if getattr(req, "resource_type", None) not in capture_types:
                return
            try:
                resp = await req.response()
                status = resp.status if resp else None
            except Exception:
                status = None
            if status and 200 <= status < 400:
                nonlocal last_network_ts
                last_network_ts = time.monotonic()

        page.on("requestfinished", _on_request_finished)
        await page.goto(start_url)

        print("\nA browser window is open. Please log in manually.")
        print("After you finish login and see you are authenticated, return here and press Enter...")
        input()

        await context.storage_state(path=storage_state_path)
        await browser.close()
        return storage_state_path


async def crawl_contest_page(
    url: str,
    *,
    headless: bool = True,
    browser_channel: Optional[str] = "msedge",
    wait_until: str = "domcontentloaded",
    wait_for_selector: str = "body",
    timeout_ms: int = 30000,
    max_scrolls: int = 100,
    scroll_pause_ms: int = 800,
    block_images: bool = False,
) -> Response:
    """
    Minimal: open, scroll to bottom, short idle wait, collect XHR/fetch requests.
    """

    capture_types = {"xhr", "fetch"}

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless, channel=browser_channel)
        context = await browser.new_context()
        if block_images:
            async def _block_images_route(route):  # type: ignore[no-redef]
                req = route.request
                if getattr(req, "resource_type", None) == "image" or req.url.lower().endswith(
                    (".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".ico", ".avif")
                ):
                    await route.abort()
                else:
                    await route.continue_()

            await context.route("**/*", _block_images_route)
        page = await context.new_page()

        ajax_requests: List[Dict[str, Any]] = []

        async def on_request_finished(request):  # type: ignore[no-redef]
            if getattr(request, "resource_type", None) not in capture_types:
                return
            try:
                response = await request.response()
            except Exception:
                response = None
                body_raw = getattr(request, "post_data", None)
            post_params: Optional[Dict[str, Any]] = None
            if body_raw:
                try:
                    post_params = {k: v[0] if isinstance(v, list) and v else v for k, v in parse_qs(body_raw).items()}
                except Exception:
                    post_params = None
                if not post_params:
                    try:
                        post_params = json.loads(body_raw)
                    except Exception:
                        post_params = {"_raw": body_raw}

                resp_text: Optional[str] = None
                resp_json: Optional[Any] = None
                try:
                    if response:
                        resp_text = await response.text()
                        try:
                            resp_json = json.loads(resp_text)
                        except Exception:
                            resp_json = None
                except Exception:
                    resp_text = None

            ajax_requests.append(
                    {
                        "url": request.url,
                    "method": request.method,
                    "resource_type": getattr(request, "resource_type", None),
                        "status": (response.status if response else None) if response else None,
                        "post": post_params,
                        "response_text": resp_text,
                        "response_json": resp_json,
                    }
                )

        page.on("requestfinished", on_request_finished)

        response = await page.goto(url, wait_until=wait_until, timeout=timeout_ms)
        try:
            await page.wait_for_selector(wait_for_selector, timeout=timeout_ms)
        except PlaywrightTimeoutError:
            pass

        prev_height = await page.evaluate("(() => document.body.scrollHeight)")
        for _ in range(max_scrolls):
            await page.evaluate("() => window.scrollTo(0, document.body.scrollHeight)")
            await asyncio.sleep(scroll_pause_ms / 1000.0)
            new_height = await page.evaluate("(() => document.body.scrollHeight)")
            if new_height <= prev_height:
                    break
            prev_height = new_height

        try:
            await page.wait_for_load_state("networkidle")
        except Exception:
            pass

        title = await page.title()
        result: Dict[str, Any] = {
            "url": url,
            "final_url": page.url,
            "status": (response.status if response else None),
            "title": title,
            "ajax_requests": ajax_requests,
        }

        await context.close()
        await browser.close()
        return JsonResponse(
            url=url,
            body=result,
            status=response.status,
            request=response.request,
            encoding="utf-8"
        )


async def crawl_category(
    url: str,
    *,
    headless: bool = False,
    browser_channel: Optional[str] = "msedge",
    wait_until: str = "domcontentloaded",
    wait_for_selector: str = "body",
    timeout_ms: int = 90000,
    block_images: bool = False,
    filter_post_key: Optional[str] = "a",
    filter_post_value: Optional[str] = "fetch_category_data",
) -> Response:
    """
    Load a category page and capture only AJAX calls whose POST body has a=fetch_category_data.
    No scrolling performed.
    """

    capture_types = {"xhr", "fetch"}

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless, channel=browser_channel)
        context = await browser.new_context()
        if block_images:
            async def _block_images_route(route):  # type: ignore[no-redef]
                req = route.request
                if getattr(req, "resource_type", None) == "image" or req.url.lower().endswith(
                    (".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".ico", ".avif")
                ):
                    await route.abort()
                else:
                    await route.continue_()

            await context.route("**/*", _block_images_route)
        page = await context.new_page()

        ajax_requests: List[Dict[str, Any]] = []
        first_filtered_event = asyncio.Event()
        first_filtered: Optional[Dict[str, Any]] = None

        async def on_request_finished(request):  # type: ignore[no-redef]
            if getattr(request, "resource_type", None) not in capture_types:
                return
            try:
                response = await request.response()
            except Exception:
                response = None
                body_raw = getattr(request, "post_data", None)
            post_params: Optional[Dict[str, Any]] = None
            if body_raw:
                try:
                    post_params = {k: v[0] if isinstance(v, list) and v else v for k, v in parse_qs(body_raw).items()}
                except Exception:
                    post_params = None
                if not post_params:
                    try:
                        post_params = json.loads(body_raw)
                    except Exception:
                        post_params = {"_raw": body_raw}

                resp_text: Optional[str] = None
                resp_json: Optional[Any] = None
                try:
                    if response:
                        resp_text = await response.text()
                        try:
                            resp_json = json.loads(resp_text)
                        except Exception:
                            resp_json = None
                except Exception:
                    resp_text = None

            entry = {
                        "url": request.url,
                "method": request.method,
                "resource_type": getattr(request, "resource_type", None),
                        "status": (response.status if response else None) if response else None,
                        "post": post_params,
                        "response_text": resp_text,
                        "response_json": resp_json,
                    }
            ajax_requests.append(entry)

            try:
                if (
                    filter_post_key is not None
                    and filter_post_value is not None
                    and isinstance(post_params, dict)
                    and post_params.get(filter_post_key) == filter_post_value
                    and not first_filtered_event.is_set()
                ):
                    nonlocal first_filtered
                    first_filtered = entry
                    first_filtered_event.set()
            except Exception:
                pass

        page.on("requestfinished", on_request_finished)

        response = await page.goto(url, wait_until=wait_until, timeout=timeout_ms)
        try:
            await page.wait_for_selector(wait_for_selector, timeout=timeout_ms)
        except PlaywrightTimeoutError:
            pass

        # If a filtered request is expected, return as soon as we see the first one
        try:
            await asyncio.wait_for(first_filtered_event.wait(), timeout=timeout_ms / 1000.0)
        except Exception:
            # If not found in time, just proceed to a short idle
            try:
                await page.wait_for_load_state("networkidle")
            except Exception:
                pass

        title = await page.title()
        result: Dict[str, Any] = {
            "url": url,
            "final_url": page.url,
            "status": (response.status if response else None),
            "title": title,
            "ajax_requests": ajax_requests,
            "first_filtered": first_filtered,
        }

        await context.close()
        await browser.close()
        return JsonResponse(
            url=url,
            body=result,
            status=response.status,
            request=response.request,
            encoding="utf-8"
        )


async def crawl_post(
    url: str,
    *,
    headless: bool = False,
    browser_channel: Optional[str] = "msedge",
    wait_until: str = "domcontentloaded",
    wait_for_selector: str = "body",
    timeout_ms: int = 30000,
    max_scrolls: int = 60,
    scroll_pause_ms: int = 100,
    initial_wait_ms: int = 0,
    stop_settle_ms: int = 10000,
    block_images: bool = False,
    scroll_selector: str,
    ready_xpath: Optional[str] = None,
    ready_timeout_ms: int = 15000,
) -> Response:
    """
    Minimal DOM crawler for posts:
    - Load page and scroll to bottom to trigger dynamic loads
    - Return the fully rendered HTML for downstream parsing
    """

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless, channel=browser_channel)
        context = await browser.new_context()
        if block_images:
            async def _block_images_route(route):  # type: ignore[no-redef]
                req = route.request
                if getattr(req, "resource_type", None) == "image" or req.url.lower().endswith(
                    (".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".ico", ".avif")
                ):
                    await route.abort()
                else:
                    await route.continue_()

            await context.route("**/*", _block_images_route)
        page = await context.new_page()

        # # Network-based approach (commented out)
        # last_request_time = time.monotonic()
        # capture_types = {"xhr", "fetch"}
        # ongoing_requests = {}  # req -> start_time

        # def _on_request(req):  # type: ignore[no-redef]
        #     nonlocal last_request_time
        #     if getattr(req, "resource_type", None) in capture_types:
        #         last_request_time = time.monotonic()
        #         ongoing_requests[req] = time.monotonic()

        # async def _on_request_finished(req):  # type: ignore[no-redef]
        #     if getattr(req, "resource_type", None) in capture_types:
        #         ongoing_requests.pop(req, None)

        # async def _on_request_failed(req):  # type: ignore[no-redef]
        #     if getattr(req, "resource_type", None) in capture_types:
        #         ongoing_requests.pop(req, None)

        # page.on("request", _on_request)
        # page.on("requestfinished", _on_request_finished)
        # page.on("requestfailed", _on_request_failed)

        response = await page.goto(url, wait_until=wait_until, timeout=timeout_ms)
        try:
            await page.wait_for_selector(wait_for_selector, timeout=timeout_ms)
        except PlaywrightTimeoutError:
            pass

        # Optional: wait for a specific element (by XPath) to indicate UI is ready to scroll
        if ready_xpath:
            try:
                await page.wait_for_selector(
                    ready_xpath if ready_xpath.startswith("xpath=") else f"xpath={ready_xpath}",
                    timeout=ready_timeout_ms,
                )
            except PlaywrightTimeoutError:
                pass
                
        # Scroll within the target element (always XPath)
        sel = scroll_selector.strip()
        sel_for_wait = sel if sel.startswith("xpath=") else f"xpath={sel}"
        if initial_wait_ms > 0:
            try:
                await page.wait_for_selector(sel_for_wait, timeout=initial_wait_ms)
            except PlaywrightTimeoutError:
                pass
            await asyncio.sleep(initial_wait_ms / 1000.0)

        locator = page.locator(sel_for_wait)
        consecutive_no_loader_checks = 0
        last_scroll_height = 0
        
        while True:
            # Get current scroll height before scrolling
            try:
                current_scroll_height = await locator.evaluate("el => el ? el.scrollHeight : 0")
            except Exception:
                current_scroll_height = 0
            
            await locator.evaluate("el => { if (el) el.scrollBy(0, 100000); }")

            # brief yield to allow content to load
            await asyncio.sleep(max(0.02, scroll_pause_ms / 1000.0))

            # Check if aops-loader is visible
            try:
                loader = page.locator(".aops-loader")
                loader_count = await loader.count()
                loader_visible = await loader.is_visible() if loader_count > 0 else False
                print(f"aops-loader count: {loader_count}, visible: {loader_visible}, scroll height: {current_scroll_height}")
            except Exception as e:
                print(f"Error checking aops-loader: {e}")
                loader_visible = False

            # Reset counter if loader is visible
            if loader_visible:
                consecutive_no_loader_checks = 0
                last_scroll_height = current_scroll_height
            else:
                consecutive_no_loader_checks += 1

            # Only consider stopping if we've had multiple consecutive checks with no loader
            # AND the scroll height hasn't changed (indicating we're truly at the bottom)
            if consecutive_no_loader_checks >= 1:
                print(f"No loader for {consecutive_no_loader_checks} checks - waiting 1 second and checking for new content...")
                await asyncio.sleep(1.0)
                
                try:
                    # Check both loader and scroll height after waiting
                    final_loader_check = await page.locator(".aops-loader").is_visible()
                    final_scroll_height = await locator.evaluate("el => el ? el.scrollHeight : 0")
                    
                    print(f"Final check - loader: {final_loader_check}, height change: {final_scroll_height - last_scroll_height}")
                    
                    # Stop only if no loader AND no height change (no new content loaded)
                    if not final_loader_check and final_scroll_height <= last_scroll_height:
                        print("No loader and no new content - stopping scroll")
                        break
                    else:
                        print("New content detected or loader appeared - continuing scroll")
                        consecutive_no_loader_checks = 0
                        last_scroll_height = final_scroll_height
                except Exception:
                    print("Error in final check - stopping scroll")
                    break


 

        try:
            html_content = await page.content()
        except Exception:
            html_content = None

        await context.close()
        await browser.close()
        return HtmlResponse(url, body=html_content, status=response.status, request=response.request, encoding="utf-8")
async def main():
    print("Starting...")
    # Example: category (no scrolling)
    # res = await crawl_category("https://artofproblemsolving.com/community/c3207_vietnam_contests")
    res = await crawl_post(
        "https://artofproblemsolving.com/community/c6h3609787p35332003",
        headless=False,
        browser_channel="msedge",
        block_images=False,
        scroll_selector="/html/body/div[1]/div[3]/div/div/div[3]/div/div[4]/div/div[2]",
        ready_xpath='//*[@id="cmty-topic-view-right"]/div/div[4]/div/div[2]/div/div[2]',
    )
    # For contests (with scrolling), call crawl_contest_page(url)
    # Save beautified JSON output to file for testing
    output_path = Path("output.html")
    with output_path.open("w", encoding="utf-8") as f:
        # make it response as a html file instead of json
        f.write(res.css("#cmty-topic-view-right > div").get())
    print(f"Saved JSON to {output_path.resolve()}")
# only run if this file is the main module
if __name__ == "__main__":
    asyncio.run(main())
