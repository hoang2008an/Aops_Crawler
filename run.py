import os
import sys
import logging

# Request asyncio reactor via env and install it BEFORE importing Scrapy/Twisted reactor users
os.environ.setdefault("TWISTED_REACTOR", "twisted.internet.asyncioreactor.AsyncioSelectorReactor")
try:
    import asyncio
    if sys.platform.startswith("win"):
        # Ensure selector-based loop policy compatible with AsyncioSelectorReactor
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    from twisted.internet import asyncioreactor
    try:
        asyncioreactor.install()
    except Exception:
        # Reactor may already be installed; ignore
        pass
except Exception:
    pass

from scrapy.crawler import CrawlerRunner
from scrapy.utils.project import get_project_settings
from scrapy.utils.log import configure_logging
from twisted.internet import reactor


def main():
    # Resolve relative paths (e.g., LOG_FILE) from project root
    project_dir = os.path.dirname(os.path.abspath(__file__))
    try:
        os.chdir(project_dir)
    except Exception:
        pass

    # Load project settings
    os.environ.setdefault("SCRAPY_SETTINGS_MODULE", "aops_crawler.settings")
    settings = get_project_settings()

    # Required by custom download handler
    settings.set(
        "TWISTED_REACTOR",
        "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
        priority="project",
    )

    # Enable job persistence so the crawl can be paused/resumed across runs
    try:
        os.makedirs(os.path.join(project_dir, "browser_data"), exist_ok=True)
    except Exception:
        pass
    settings.set("JOBDIR", os.path.join("browser_data", "job-aops"), priority="project")

    # Configure logging explicitly (CrawlerRunner does not configure logging)
    configure_logging(settings)
    # Capture stdout/stderr (print) into Scrapy log file
    settings.set("LOG_STDOUT", True, priority="project")

    runner = CrawlerRunner(settings)

    # Timebox each run and restart indefinitely
    run_for_seconds = 1800  # set to 1800 for 30 minutes

    logger = logging.getLogger(__name__)

    def start_cycle():
        logger.info("[cycle] starting new crawl")
        crawler = runner.create_crawler("aops_crawler")

        d = runner.crawl(crawler)

        # Schedule a timeboxed shutdown regardless of signals
        try:
            logger.info(f"[cycle] scheduling shutdown in {run_for_seconds}s")
            shutdown_call = reactor.callLater(run_for_seconds, _timeboxed_shutdown, crawler)
        except Exception:
            shutdown_call = None

        # When the crawl finishes (engine fully stopped), start a new cycle
        def _after_cycle(_):
            try:
                if shutdown_call is not None and shutdown_call.active():
                    shutdown_call.cancel()
            except Exception:
                pass
            try:
                logger.info("[cycle] crawl finished, restarting")
                reactor.callLater(0, start_cycle)
            except Exception:
                pass
            return _
        d.addBoth(_after_cycle)

    # Kick off the first cycle and run reactor once
    start_cycle()
    reactor.run()


def _timeboxed_shutdown(crawler):
    logging.getLogger(__name__).info("[cycle] timeboxed shutdown")
    try:
        # If engine is still running, stop it; this will resolve its Deferred
        engine = getattr(crawler, "engine", None)
        if engine is not None and getattr(engine, "running", False):
            return crawler.stop()
    except Exception:
        pass


if __name__ == "__main__":
    main()


