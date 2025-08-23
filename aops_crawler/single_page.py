from __future__ import annotations
from scrapy.http import HtmlResponse, Response, TextResponse  # <-- JsonResponse removed
import asyncio
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs
import json
import re
import html as html_module
import logging

from patchright.async_api import TimeoutError as PlaywrightTimeoutError
from patchright.async_api import async_playwright
TAGS_XPATH='/html/body/div[1]/div[3]/div/div/div[3]/div/div[3]/div[2]/div[1]/div[2]/div'

logger = logging.getLogger(__name__)

def transform_cmty_post_html(html: str) -> str:
    # 1) Replace <img ... alt="..."> with its alt text
    html = re.sub(r'<img\b[^>]*\balt="([^"]*)"[^>]*>', lambda m: m.group(1), html)
    # 2) Convert <br> (any form) to newlines
    html = re.sub(r'<br\s*/?>', '\n', html, flags=re.I)
    # 3) Convert <i>...</i> to [i]...[/i]
    html = re.sub(r'<i\b[^>]*>', '[i]', html, flags=re.I)
    html = re.sub(r'</i>', '[/i]', html, flags=re.I)
    # 3.5) Explicitly unwrap span wrappers (e.g., white-space:pre spans AoPS adds)
    html = re.sub(r'</?span\b[^>]*>', '', html, flags=re.I)
    # 4) Strip other tags but keep their text
    html = re.sub(r'</?div\b[^>]*>', '', html, flags=re.I)
    # Generic tag remover (last resort) while preserving text
    html = re.sub(r'<[^>]+>', '', html)
    # 5) Unescape HTML entities
    html = html_module.unescape(html)
    # Normalize consecutive newlines/spaces
    html = re.sub(r'\s+\n', '\n', html).strip()
    return html

# async def create_storage_state_interactive(
#     *,
#     storage_state_path: str = "state.json",
#     start_url: str = "https://artofproblemsolving.com/",
#     headless: bool = False,
#     browser_channel: Optional[str] = "msedge",
# ) -> str:
#     async with async_playwright() as p:
#         browser = await p.chromium.launch(headless=headless, channel=browser_channel)
#         page = await browser.new_page()
#
#         last_network_ts = time.monotonic()
#         capture_types = {"xhr", "fetch"}
#
#         async def _on_request_finished(req):
#             if getattr(req, "resource_type", None) not in capture_types:
#                 return
#             try:
#                 resp = await req.response()
#                 status = resp.status if resp else None
#             except Exception:
#                 status = None
#             if status and 200 <= status < 400:
#                 nonlocal last_network_ts
#                 last_network_ts = time.monotonic()
#
#         page.on("requestfinished", _on_request_finished)
#         await page.goto(start_url)
#
#         print("\nA browser window is open. Please log in manually.")
#         print("After you finish login and see you are authenticated, return here and press Enter...")
#         input()
#
#         await context.storage_state(path=storage_state_path)
#         await browser.close()
#         return storage_state_path


async def crawl_contest_page(
    url: str,
    browser,
    *,
    wait_until: str = "domcontentloaded",
    wait_for_selector: str = "body",
    timeout_ms: int = 30000,
    max_scrolls: int = 10,
    scroll_pause_ms: int = 800,
    block_images: bool = False,
) -> Response:
    capture_types = {"xhr", "fetch"}

    page = await browser.new_page()
    ajax_requests: List[Dict[str, Any]] = []

    async def on_request_finished(request):
        if getattr(request, "resource_type", None) not in capture_types:
            return
        # ---- initialize everything up front
        body_raw: Optional[str] = getattr(request, "post_data", None)
        post_params: Optional[Dict[str, Any]] = None
        resp_text: Optional[str] = None
        resp_json: Optional[Any] = None
        response = None

        try:
            response = await request.response()
        except Exception:
            response = None  # keep going; we still have body_raw if present

        # parse POST body if present
        if body_raw:
            try:
                post_params = {k: v[0] if isinstance(v, list) and v else v
                               for k, v in parse_qs(body_raw).items()}
            except Exception:
                post_params = None
            if not post_params:
                try:
                    post_params = json.loads(body_raw)
                except Exception:
                    post_params = {"_raw": body_raw}

        # capture response text/json
        try:
            if response:
                resp_text = await response.text()
                try:
                    resp_json = json.loads(resp_text)
                except Exception:
                    resp_json = None
        except Exception:
            resp_text = None

        ajax_requests.append({
            "url": request.url,
            "method": request.method,
            "resource_type": getattr(request, "resource_type", None),
            "status": (response.status if response else None),
            "post": post_params,
            "response_text": resp_text,
            "response_json": resp_json,
        })

    page.on("requestfinished", on_request_finished)

    response = await page.goto(url, wait_until=wait_until, timeout=timeout_ms)
    try:
        await page.wait_for_selector(wait_for_selector, timeout=timeout_ms)
    except PlaywrightTimeoutError:
        pass

    for _ in range(max_scrolls):
        await page.evaluate("() => window.scrollTo(0, document.body.scrollHeight)")
        await asyncio.sleep(scroll_pause_ms / 1000.0)
        # if new_height <= prev_height:
        #     break
        # prev_height = new_height

    # try:
    #     await page.wait_for_load_state("networkidle")
    # except Exception:
    #     pass

    title = await page.title()
    result: Dict[str, Any] = {
        "url": url,
        "final_url": page.url,
        "status": (response.status if response else None),
        "title": title,
        "ajax_requests": ajax_requests,
    }

    await page.close()

    # ---- Scrapy TextResponse with BYTES body
    body_bytes = json.dumps(result, ensure_ascii=False).encode("utf-8")
    return TextResponse(
        url=url,
        body=body_bytes,
        status=(response.status if response else 200),
        encoding="utf-8",
        headers={"Content-Type": "application/json; charset=utf-8"},
    )


async def crawl_category(
    url: str,
    browser,
    *,
    wait_until: str = "domcontentloaded",
    wait_for_selector: str = "body",
    timeout_ms: int = 90000,
    block_images: bool = False,
    filter_post_key: Optional[str] = "a",
    filter_post_value: Optional[str] = "fetch_category_data",
) -> Response:
    capture_types = {"xhr", "fetch"}
    page = await browser.new_page()
    ajax_requests: List[Dict[str, Any]] = []
    first_filtered_event = asyncio.Event()
    first_filtered: Optional[Dict[str, Any]] = None

    async def on_request_finished(request):

        if getattr(request, "resource_type", None) not in capture_types:
            return
        # print(request.url)
        body_raw: Optional[str] = getattr(request, "post_data", None)
        post_params: Optional[Dict[str, Any]] = None
        resp_text: Optional[str] = None
        resp_json: Optional[Any] = None
        response = None

        try:
            response = await request.response()
        except Exception:
            response = None

        if body_raw:
            try:
                post_params = {k: v[0] if isinstance(v, list) and v else v
                               for k, v in parse_qs(body_raw).items()}
            except Exception:
                post_params = None
            if not post_params:
                try:
                    post_params = json.loads(body_raw)
                except Exception:
                    post_params = {"_raw": body_raw}

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
            "status": (response.status if response else None),
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
                # print("first_filtered")
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

    try:
        await asyncio.wait_for(first_filtered_event.wait(), timeout=timeout_ms / 1000.0)
    except Exception:
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

    await page.close()

    body_bytes = json.dumps(result, ensure_ascii=False).encode("utf-8")
    return TextResponse(
        url=url,
        body=body_bytes,
        status=(response.status if response else 200),
        encoding="utf-8",
        headers={"Content-Type": "application/json; charset=utf-8"},
    )


async def crawl_post(
    url: str,
    browser,
    *,
    wait_until: str = "domcontentloaded",
    wait_for_selector: str = "body",
    timeout_ms: int = 30000,
    max_scrolls: int = 60,
    scroll_pause_ms: int = 100,
    initial_wait_ms: int = 0,
    stop_settle_ms: int = 10000,
    block_images: bool = False,
    scroll_selector:str = "/html/body/div[1]/div[3]/div/div/div[3]/div/div[4]/div/div[2]",
    ready_xpath: str = '//*[@id="cmty-topic-view-right"]/div/div[4]/div/div[2]/div/div[2]',
    ready_timeout_ms: int = 15000,
) -> Response:
    page = await browser.new_page()
    if block_images:
        async def _block_images_route(route):
            req = route.request
            if getattr(req, "resource_type", None) == "image" or req.url.lower().endswith(
                (".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".ico", ".avif")
            ):
                await route.abort()
            else:
                await route.continue_()
        await page.route("**/*", _block_images_route)

    response = await page.goto(url, wait_until=wait_until, timeout=timeout_ms)
    try:
        await page.wait_for_selector(wait_for_selector, timeout=timeout_ms)
    except PlaywrightTimeoutError:
        pass

    if ready_xpath:
        try:
            await page.wait_for_selector(
                ready_xpath if ready_xpath.startswith("xpath=") else f"xpath={ready_xpath}",
                timeout=ready_timeout_ms,
            )
        except PlaywrightTimeoutError:
            pass

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
        try:
            current_scroll_height = await locator.evaluate("el => el ? el.scrollHeight : 0")
        except Exception:
            current_scroll_height = 0

        # Scroll element (use scrollTop for compatibility)
        await locator.evaluate("el => { if (el) el.scrollTop = el.scrollHeight; }")

        await asyncio.sleep(max(0.02, scroll_pause_ms / 1000.0))

        try:
            loader = page.locator(".aops-loader")
            loader_count = await loader.count()
            loader_visible = await loader.is_visible() if loader_count > 0 else False
            # print(f"aops-loader count: {loader_count}, visible: {loader_visible}, scroll height: {current_scroll_height}")
        except Exception as e:
            # print(f"Error checking aops-loader: {e}")
            loader_visible = False

        if loader_visible:
            consecutive_no_loader_checks = 0
            last_scroll_height = current_scroll_height
        else:
            consecutive_no_loader_checks += 1

        if consecutive_no_loader_checks >= 1:
            # print("No loader for 1 checks - waiting 1 second and checking for new content...")
            await asyncio.sleep(1.0)
            try:
                final_loader_check = await page.locator(".aops-loader").is_visible()
                final_scroll_height = await locator.evaluate("el => el ? el.scrollHeight : 0")
                # print(f"Final check - loader: {final_loader_check}, height change: {final_scroll_height - last_scroll_height}")
                if not final_loader_check and final_scroll_height <= last_scroll_height:
                    # print("No loader and no new content - stopping scroll")
                    break
                else:
                    # print("New content detected or loader appeared - continuing scroll")
                    consecutive_no_loader_checks = 0
                    last_scroll_height = final_scroll_height
            except Exception:
                # print("Error in final check - stopping scroll")
                break

    try:
        html_content = await page.content()
    except Exception:
        html_content = ""

    await page.close()

    # Ensure bytes + correct URL/status
    return HtmlResponse(
        url=(response.url if response else url),
        body=(html_content or "").encode("utf-8"),
        status=(response.status if response else 200),
        encoding="utf-8",
        headers={"Content-Type": "text/html; charset=utf-8"},
    )


async def main():
    # print("Starting...")
    async with async_playwright() as p:
        browser = await p.chromium.launch_persistent_context(headless=False, channel="msedge", no_viewport=True, user_data_dir="./browser_data/msedge")
        res = await crawl_post(
            "https://artofproblemsolving.com/community/c6h3358923p31205921",
            browser=browser,
            # scroll_selector="/html/body/div[1]/div[3]/div/div/div[3]/div/div[4]/div/div[2]",
            # ready_xpath='//*[@id="cmty-topic-view-right"]/div/div[4]/div/div[2]/div/div[2]',
        )
        # print(res.xpath('normalize-space(substring-after(string(//div[contains(@class,"cmty-topic-source-display")]), ":"))').get())
        tag_texts = [
            t.strip()
            for t in res.xpath(f'{TAGS_XPATH}//a/div[contains(@class,"cmty-item-tag")]/text()').getall()
            if t and t.strip()
        ]
        # print(tag_texts)
        for item in res.xpath('/html/body/div[1]/div[3]/div/div/div[3]/div/div[4]/div/div[2]/div').xpath('./div[contains(@class, "cmty-post")]'):
            user_id = item.xpath('normalize-space(substring-after((.//a[starts-with(@href,"/community/user/")]/@href)[1], "/community/user/"))').get()
            mid = item.xpath('.//div[contains(@class,"cmty-post-middle")]')
            created_text = mid.xpath('normalize-space(.//span[contains(@class,"cmty-post-date")])').get()
            thanks_raw = mid.xpath('.//span[contains(@class,"cmty-post-thank-count")]//text()').re_first(r'(\\d+)')
            thanks_count = int(thanks_raw) if thanks_raw else 0
            no_thanks_raw = mid.xpath('.//span[contains(@class,"cmty-post-nothank-count")]//text()').re_first(r'(\\d+)')
            no_thanks_count = int(no_thanks_raw) if no_thanks_raw else 0
            post_html = ''.join(item.xpath('.//div[contains(@class,"cmty-post-html")]/node()').getall()).strip()
            if user_id=='971975':
                # print({
                #     "user_id": user_id,
                #     "created_text": created_text,
                #     "thanks_count": thanks_count,
                #     "no_thanks_count": no_thanks_count,
                #     "post_html": post_html,
                #     "post_text": transform_cmty_post_html(post_html),
                # })
                pass


if __name__ == "__main__":
    asyncio.run(main())
