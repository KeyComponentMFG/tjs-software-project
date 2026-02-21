"""Gunicorn config for Railway deployment."""
import threading
import time
import urllib.request


def post_worker_init(worker):
    """After gunicorn worker starts, reload data from Supabase in background.

    This ensures the worker always has the latest data, even if a previous
    deployment cached stale data.
    """
    def _reload():
        time.sleep(3)  # wait for server to be ready
        try:
            port = worker.cfg.bind[0].split(":")[-1] if worker.cfg.bind else "8000"
            url = f"http://127.0.0.1:{port}/api/reload"
            urllib.request.urlopen(url, timeout=30)
            worker.log.info("Auto-reloaded data from Supabase")
        except Exception as e:
            worker.log.warning(f"Auto-reload failed: {e}")

    t = threading.Thread(target=_reload, daemon=True)
    t.start()
