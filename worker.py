"""
worker.py — DBS Redis Queue Worker
Runs as a separate process on Render.
Start command: python worker.py
"""
import os, logging, sys
from redis import Redis
from rq import Worker, Queue, Connection

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s — %(message)s")
log = logging.getLogger(__name__)

REDIS_URL      = os.environ.get("REDIS_URL", "redis://localhost:6379")
DBS_QUEUE_NAME = "nextstep:dbs:jobs"

def main():
    log.info("[Worker] DBS worker starting — queue: %s", DBS_QUEUE_NAME)
    redis_conn = Redis.from_url(REDIS_URL)
    try:
        redis_conn.ping()
        log.info("[Worker] Redis OK")
    except Exception as e:
        log.error("[Worker] Redis FAILED: %s", e)
        sys.exit(1)
    with Connection(redis_conn):
        worker = Worker(queues=[DBS_QUEUE_NAME], connection=redis_conn, log_job_description=True)
        log.info("[Worker] Ready — waiting for jobs...")
        worker.work(with_scheduler=True)

if __name__ == "__main__":
    main()
