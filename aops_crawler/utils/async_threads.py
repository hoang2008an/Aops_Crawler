from twisted.internet.threads import deferToThread


def run_coro_in_proactor_thread(coro):
    """
    Run an asyncio coroutine in a separate thread with a Windows Proactor
    event loop (so subprocess works), return a Twisted Deferred.
    """
    def _runner():
        import asyncio
        import sys
        if sys.platform.startswith("win"):
            asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
        return asyncio.run(coro)  # runs the whole async function inside the thread
    return deferToThread(_runner)


# ---- Single persistent Proactor event loop for Playwright reuse ----
_BG_LOOP = None
_BG_THREAD = None


def start_background_proactor_loop():
    """Start a single persistent background Proactor event loop if not running."""
    def _start():
        import asyncio
        import threading
        import sys
        global _BG_LOOP, _BG_THREAD
        if _BG_LOOP is not None:
            return True
        if sys.platform.startswith("win"):
            asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
        loop = asyncio.new_event_loop()
        def _run():
            asyncio.set_event_loop(loop)
            loop.run_forever()
        thread = threading.Thread(target=_run, name="bg-proactor-loop", daemon=True)
        thread.start()
        _BG_LOOP = loop
        _BG_THREAD = thread
        return True
    return deferToThread(_start)


def run_coro_on_background_loop(coro):
    """Schedule a coroutine on the persistent background loop, return Deferred."""
    def _submit():
        import asyncio
        global _BG_LOOP
        if _BG_LOOP is None:
            # start synchronously in this worker thread
            import sys
            if sys.platform.startswith("win"):
                asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
            from threading import Event, Thread
            loop = asyncio.new_event_loop()
            ready = Event()
            def _run():
                asyncio.set_event_loop(loop)
                ready.set()
                loop.run_forever()
            t = Thread(target=_run, name="bg-proactor-loop", daemon=True)
            t.start()
            ready.wait()  # Wait for loop to be ready before proceeding
            # publish
            global _BG_THREAD
            _BG_LOOP = loop
            _BG_THREAD = t
        fut = asyncio.run_coroutine_threadsafe(coro, _BG_LOOP)
        return fut.result()
    return deferToThread(_submit)


def stop_background_proactor_loop():
    """Stop the persistent background loop and join the thread."""
    def _stop():
        global _BG_LOOP, _BG_THREAD
        loop = _BG_LOOP
        thread = _BG_THREAD
        _BG_LOOP = None
        _BG_THREAD = None
        if loop is None:
            return True
        try:
            loop.call_soon_threadsafe(loop.stop)
        except Exception:
            pass
        if thread is not None:
            try:
                thread.join(timeout=5)
            except Exception:
                pass
        try:
            loop.close()
        except Exception:
            pass
        return True
    return deferToThread(_stop)