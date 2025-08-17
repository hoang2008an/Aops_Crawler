import importlib
from typing import Any, Callable, Optional, Tuple
from twisted.internet.defer import ensureDeferred
from scrapy.core.downloader.handlers.http11 import HTTP11DownloadHandler
from single_page import crawl_contest_page, crawl_category, crawl_post
FetchResult = Tuple[Any, Optional[str], Optional[int], Optional[dict]]

def _load(path: str) -> Callable:
    mod, name = path.rsplit(".", 1)
    return getattr(importlib.import_module(mod), name)

def _to_bytes(b) -> bytes:
    return b if isinstance(b, (bytes, bytearray)) else str(b).encode("utf-8")

class MultiDownloadHandler:
    """
    Routes per request by request.meta["driver"]:
      - "contest"     -> contest page
      - "category"  -> category page
      - "post"      -> post page
    """

    def __init__(self, settings, crawler=None):
        self._http = HTTP11DownloadHandler(settings, crawler)
        self.browser_fetch = _load(settings.get("MY_BROWSER_FETCH"))
        self.api_fetch     = _load(settings.get("MY_API_FETCH"))

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings, crawler)

    def download_request(self, request, spider):
        driver = request.meta.get("driver", "http")
        if driver == "http":
            return self._http.download_request(request, spider)
        if driver == "contest":
            return ensureDeferred(crawl_contest_page(request.url))
        if driver == "category":
            return ensureDeferred(crawl_category(request.url))
        if driver == "post":
            return ensureDeferred(crawl_post(request.url))

        # unknown -> fallback
        return self._http.download_request(request, spider)

    
