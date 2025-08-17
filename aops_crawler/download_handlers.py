 

from twisted.internet.defer import Deferred
from scrapy.core.downloader.handlers.http import HTTPDownloadHandler
from scrapy.http import Request
from scrapy import signals
from scrapy.utils.defer import deferred_from_coro
from scrapy.utils.reactor import verify_installed_reactor
from aops_crawler.single_page import crawl_contest_page, crawl_category, crawl_post
from patchright.async_api import async_playwright
from aops_crawler.utils.async_threads import (
    run_coro_on_background_loop,
    start_background_proactor_loop,
)

__all__ = ["ScrapyPatchrightDownloadHandler"]


class ScrapyPatchrightDownloadHandler(HTTPDownloadHandler):
    """
    Minimal handler that follows your example’s structure but delegates the actual
    downloading/rendering to *your* functions that already manage Patchright.
    Routes by request.meta["driver"]:
      - "http"     -> fallback to HTTPDownloadHandler
      - "browser"  -> call PATCHRIGHT_HTML_FETCH, wrap as HtmlResponse if needed
      - "api"      -> call PATCHRIGHT_API_FETCH,  wrap as TextResponse if needed
    """

    def __init__(self, settings, crawler=None) -> None:
        super().__init__(settings=settings, crawler=crawler)
        verify_installed_reactor("twisted.internet.asyncioreactor.AsyncioSelectorReactor")
        self._crawler = crawler

        crawler.signals.connect(self._engine_started, signal=signals.engine_started)
        crawler.signals.connect(self._engine_stopped, signal=signals.engine_stopped)
        # shared browser context created on engine start and reused across requests
        self._shared_ctx = None
        self._browser = None  # only used when falling back to non-persistent
        self._browser_channel = crawler.settings.get("AOPS_BROWSER_CHANNEL", "msedge")
        self._headless = crawler.settings.getbool("AOPS_HEADLESS", False)

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings, crawler)

    # ---- helpers mirroring your example ----
    def _deferred_from_coro(self, coro) -> Deferred:
        return deferred_from_coro(coro)

    def _engine_started(self) -> Deferred:
        # Start a persistent Proactor loop and a single shared browser context
        def _launch():
            async def _run():
                # Reuse manager if already started
                if getattr(self, "_p", None) is None:
                    p_mgr = async_playwright()
                    self._p_mgr = p_mgr
                    self._p = await p_mgr.start()
                # If context already exists, nothing to do
                if self._shared_ctx is not None:
                    return None
                # Try to launch persistent context; retry on transient failure
                for _ in range(2):
                    try:
                        self._shared_ctx = await self._p.chromium.launch_persistent_context(
                            headless=self._headless,
                            channel=self._browser_channel,
                            no_viewport=True,
                            user_data_dir=f"./browser_data/{self._browser_channel}",
                        )
                        break
                    except Exception:
                        # small delay and retry
                        import asyncio as _a
                        await _a.sleep(0.5)
                # Fallback: non-persistent browser/context
                if self._shared_ctx is None:
                    self._browser = await self._p.chromium.launch(
                        headless=self._headless,
                        channel=self._browser_channel,
                    )
                    self._shared_ctx = await self._browser.new_context(no_viewport=True)
                return None
            return run_coro_on_background_loop(_run())
        # ensure loop is running first
        d1 = start_background_proactor_loop()
        def _after_start(_):
            return _launch()
        d1.addCallback(_after_start)
        return d1

    def _engine_stopped(self) -> Deferred:
        # Close shared context; keep background loop alive to avoid WinError 995
        async def _close():
            try:
                if self._shared_ctx is not None:
                    # Close context more gently
                    try:
                        await self._shared_ctx.close()
                    except Exception as e:
                        # Log but don't fail - context might already be closed
                        print(f"Warning: Context close failed: {e}")
            except Exception:
                pass
            finally:
                self._shared_ctx = None
                
            try:
                if self._browser is not None:
                    try:
                        await self._browser.close()
                    except Exception as e:
                        print(f"Warning: Browser close failed: {e}")
            except Exception:
                pass
            finally:
                self._browser = None
            return None
        return run_coro_on_background_loop(_close())

    # ---- Scrapy entry point ----
    def download_request(self, request: Request, spider) -> Deferred:
        driver = request.meta.get("driver", "http")
        print(driver)
        print("--------------------------------------------------")
        if driver == "contest":
            async def _run():
                import asyncio
                # wait until shared context is ready
                while self._shared_ctx is None:
                    await asyncio.sleep(0.05)
                return await crawl_contest_page(
                    request.url,
                    browser=self._shared_ctx,
                    wait_until=request.meta.get("wait_until", "domcontentloaded"),
                    wait_for_selector=request.meta.get("wait_for_selector", "body"),
                    timeout_ms=request.meta.get("timeout_ms", 30000),
                    max_scrolls=request.meta.get("max_scrolls", 10),
                    scroll_pause_ms=request.meta.get("scroll_pause_ms", 800),
                    block_images=request.meta.get("block_images", False),
                )
            return run_coro_on_background_loop(_run())
        if driver == "category":
            async def _run():
                import asyncio
                while self._shared_ctx is None:
                    await asyncio.sleep(0.05)
                return await crawl_category(
                    request.url,
                    browser=self._shared_ctx,
                    wait_until=request.meta.get("wait_until", "domcontentloaded"),
                    wait_for_selector=request.meta.get("wait_for_selector", "body"),
                    timeout_ms=request.meta.get("timeout_ms", 90000),
                    block_images=request.meta.get("block_images", False),
                    filter_post_key=request.meta.get("filter_post_key", "a"),
                    filter_post_value=request.meta.get("filter_post_value", "fetch_category_data"),
                )
            return run_coro_on_background_loop(_run())
        if driver == "post":
            async def _run():
                import asyncio
                while self._shared_ctx is None:
                    await asyncio.sleep(0.05)
                return await crawl_post(
                    request.url,
                    browser=self._shared_ctx,
                    wait_until=request.meta.get("wait_until", "domcontentloaded"),
                    wait_for_selector=request.meta.get("wait_for_selector", "body"),
                    timeout_ms=request.meta.get("timeout_ms", 30000),
                    max_scrolls=request.meta.get("max_scrolls", 60),
                    scroll_pause_ms=request.meta.get("scroll_pause_ms", 100),
                    initial_wait_ms=request.meta.get("initial_wait_ms", 0),
                    stop_settle_ms=request.meta.get("stop_settle_ms", 10000),
                    block_images=request.meta.get("block_images", False),
                    scroll_selector=request.meta["scroll_selector"],
                    ready_xpath=request.meta.get("ready_xpath"),
                    ready_timeout_ms=request.meta.get("ready_timeout_ms", 15000),
                )
            return run_coro_on_background_loop(_run())

        # unknown -> fallback
        return super().download_request(request, spider)
# class ScrapyPatchrightDownloadHandler(HTTPDownloadHandler):
#     def download_request(self, request, spider):
#         # Custom header added to request
#         print(request.meta)
#         print("--------------------------------------------------")
#         request.headers.setdefault(b'Authorization', b'Bearer mysecrettoken')
#         response = super().download_request(request, spider)
#         # Custom processing after download
 
#         return response