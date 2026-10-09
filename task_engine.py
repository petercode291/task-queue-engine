"""Task Queue Engine: a lightweight async job scheduler with a live dashboard.

Run it with:  python task_engine.py            (empty queue)
              python task_engine.py --demo     (queues a burst of demo tasks)
"""
import asyncio
import contextlib
import inspect
import itertools
import json
import logging
import os
import sqlite3
import sys
import time
from collections.abc import AsyncIterator, Callable, Iterator
from concurrent.futures import ProcessPoolExecutor
from contextlib import asynccontextmanager
from enum import Enum
from pathlib import Path
from typing import Any
from uuid import uuid4

import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel, ConfigDict, Field

if sys.version_info < (3, 11):
    raise RuntimeError(
        "Task Queue Engine needs Python 3.11 or newer (it uses asyncio.timeout).")

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("TaskEngine")

# The database path can be overridden, which is how the tests avoid touching real data.
DB_PATH = os.environ.get("TASK_DB", "tasks.db")
DASHBOARD_PATH = Path(__file__).resolve().parent / "dashboard.html"
DEMO_MODE = "--demo" in sys.argv
MAX_ERROR_LENGTH = 500


# ==========================================
# 1. Models & Database
# ==========================================
class TaskStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    RETRYING = "RETRYING"


class TaskModel(BaseModel):
    """A task as stored in the database and held in the queue."""

    id: str = Field(default_factory=lambda: str(uuid4())[:8])
    func_name: str
    args: list[Any] = Field(default_factory=list)
    priority: int = 10  # lower number = runs first
    max_retries: int = 3
    timeout: int = 10  # seconds
    status: TaskStatus = TaskStatus.PENDING
    retries: int = 0
    error: str | None = None
    created_at: float = Field(default_factory=time.time)
    started_at: float | None = None
    completed_at: float | None = None


class TaskCreate(BaseModel):
    """What an API client is allowed to set when submitting a task.

    The server decides the id, status and timestamps, so clients cannot
    overwrite existing tasks or inject their own values.
    """

    model_config = ConfigDict(extra="forbid")

    func_name: str
    args: list[Any] = Field(default_factory=list)
    priority: int = Field(default=10, ge=0, le=1000)
    max_retries: int = Field(default=3, ge=0, le=10)
    timeout: int = Field(default=10, ge=1, le=3600)


@contextlib.contextmanager
def get_db() -> Iterator[sqlite3.Connection]:
    """Open a connection, commit on success, roll back on error, always close."""
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    try:
        with conn:
            yield conn
    finally:
        conn.close()


def init_db() -> None:
    with get_db() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS tasks (
                id TEXT PRIMARY KEY,
                func_name TEXT,
                args TEXT,
                priority INTEGER,
                max_retries INTEGER,
                timeout INTEGER,
                status TEXT,
                retries INTEGER,
                error TEXT,
                created_at REAL,
                started_at REAL,
                completed_at REAL
            )
            """
        )
        # Upgrade databases created before the timestamp columns existed.
        existing = {row["name"]
                    for row in conn.execute("PRAGMA table_info(tasks)")}
        for column in ("created_at", "started_at", "completed_at"):
            if column not in existing:
                conn.execute(f"ALTER TABLE tasks ADD COLUMN {column} REAL")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status)")


def row_to_task(row: sqlite3.Row) -> TaskModel:
    """Turn a database row back into a TaskModel (args are stored as JSON text)."""
    data = dict(row)
    data["args"] = json.loads(data["args"] or "[]")
    if not data.get("created_at"):
        data["created_at"] = time.time()
    return TaskModel(**data)


# ==========================================
# 2. Core Engine
# ==========================================
class TaskQueueEngine:
    def __init__(self, concurrency: int = 3) -> None:
        self.concurrency = concurrency
        self.backoff_multiplier = 1.0  # tests lower this to run retries quickly
        # tie-breaker: equal priorities run first-in, first-out
        self._counter = itertools.count()
        self._queue: asyncio.PriorityQueue[tuple[int,
                                                 int, TaskModel]] = asyncio.PriorityQueue()
        self._workers: list[asyncio.Task[None]] = []
        # strong references so they are not garbage collected
        self._retry_tasks: set[asyncio.Task[None]] = set()
        self._process_pool: ProcessPoolExecutor | None = None
        self._func_registry: dict[str, Callable[..., Any]] = {}
        self._is_running = False

    @property
    def is_running(self) -> bool:
        return self._is_running

    def register_function(self, name: str, func: Callable[..., Any]) -> None:
        self._func_registry[name] = func

    def has_function(self, name: str) -> bool:
        return name in self._func_registry

    def _save_task(self, task: TaskModel) -> None:
        with get_db() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO tasks
                (id, func_name, args, priority, max_retries, timeout, status, retries,
                 error, created_at, started_at, completed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    task.id, task.func_name, json.dumps(
                        task.args), task.priority,
                    task.max_retries, task.timeout, task.status.value, task.retries,
                    task.error, task.created_at, task.started_at, task.completed_at,
                ),
            )

    def _enqueue(self, task: TaskModel) -> None:
        self._queue.put_nowait((task.priority, next(self._counter), task))

    def submit(self, task: TaskModel) -> str:
        if task.func_name not in self._func_registry:
            raise ValueError(f"Unknown function: {task.func_name}")
        self._save_task(task)
        if task.status in (TaskStatus.PENDING, TaskStatus.RETRYING):
            self._enqueue(task)
            logger.info("Task %s queued (%s, priority %d)",
                        task.id, task.func_name, task.priority)
        return task.id

    async def _execute_task(self, task: TaskModel) -> None:
        func = self._func_registry.get(task.func_name)
        if func is None:
            raise ValueError(f"Function {task.func_name} not registered.")
        if inspect.iscoroutinefunction(func):
            await func(*task.args)
        else:
            if self._process_pool is None:
                raise RuntimeError("Engine is not running.")
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(self._process_pool, func, *task.args)

    async def _run_task(self, task: TaskModel, worker: str) -> None:
        task.status = TaskStatus.RUNNING
        task.started_at = time.time()
        task.completed_at = None
        self._save_task(task)
        logger.info("%s executing task %s (%s)",
                    worker, task.id, task.func_name)

        try:
            async with asyncio.timeout(task.timeout):
                await self._execute_task(task)
        except Exception as exc:  # CancelledError is not an Exception, so shutdown passes through
            self._handle_failure(task, exc)
            return

        task.status = TaskStatus.COMPLETED
        task.completed_at = time.time()
        self._save_task(task)
        logger.info("%s finished task %s", worker, task.id)

    def _handle_failure(self, task: TaskModel, exc: Exception) -> None:
        task.error = (str(exc) or type(exc).__name__)[:MAX_ERROR_LENGTH]
        task.completed_at = time.time()

        if task.retries < task.max_retries:
            task.retries += 1
            task.status = TaskStatus.RETRYING
            self._save_task(task)
            delay = (2 ** task.retries) * self.backoff_multiplier
            logger.warning("Task %s failed (%s); retry %d/%d in %.1fs",
                           task.id, task.error, task.retries, task.max_retries, delay)
            retry = asyncio.create_task(self._schedule_retry(task, delay))
            self._retry_tasks.add(retry)
            retry.add_done_callback(self._retry_tasks.discard)
        else:
            task.status = TaskStatus.FAILED
            self._save_task(task)
            logger.error("Task %s permanently failed: %s", task.id, task.error)

    async def _schedule_retry(self, task: TaskModel, delay: float) -> None:
        await asyncio.sleep(delay)
        task.status = TaskStatus.PENDING
        task.started_at = None
        task.completed_at = None
        self.submit(task)

    async def _worker_loop(self, worker_id: int) -> None:
        name = f"Worker-{worker_id}"
        logger.info("%s started", name)
        while self._is_running:
            try:
                _, _, task = await self._queue.get()
            except asyncio.CancelledError:
                break
            try:
                await self._run_task(task, name)
            except asyncio.CancelledError:
                logger.info("%s stopped while running task %s; it is recovered on the next start",
                            name, task.id)
                break
            except Exception:
                # e.g. a locked database: log it and keep the worker alive
                logger.exception(
                    "%s hit an unexpected error on task %s", name, task.id)
            finally:
                self._queue.task_done()

    async def start(self) -> None:
        if self._is_running:
            return
        init_db()
        self._is_running = True
        self._queue = asyncio.PriorityQueue()  # the database is the source of truth
        self._process_pool = ProcessPoolExecutor()

        with get_db() as conn:
            # Tasks that were mid-run when the engine stopped go back to the queue.
            conn.execute(
                "UPDATE tasks SET status = 'PENDING', started_at = NULL WHERE status = 'RUNNING'")
            rows = conn.execute(
                "SELECT * FROM tasks WHERE status IN ('PENDING', 'RETRYING')"
            ).fetchall()
        for row in rows:
            self._enqueue(row_to_task(row))
        if rows:
            logger.info(
                "Recovered %d unfinished task(s) from the database", len(rows))

        self._workers = [asyncio.create_task(
            self._worker_loop(i)) for i in range(self.concurrency)]

    async def stop(self) -> None:
        self._is_running = False
        pending = [*self._workers, *self._retry_tasks]
        for t in pending:
            t.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        self._workers.clear()
        if self._process_pool is not None:
            self._process_pool.shutdown(wait=False, cancel_futures=True)
            self._process_pool = None
        logger.info("Engine stopped")


# ==========================================
# 3. Demo functions & FastAPI app
# ==========================================
async def async_network_call(url: str) -> str:
    await asyncio.sleep(1)  # stands in for a real HTTP request
    return f"Fetched {url}"


def sync_cpu_heavy(n: int) -> int:
    time.sleep(2)  # stands in for heavy computation; runs in the process pool
    if n == 13:
        raise ValueError("Unlucky number 13 crash!")
    return n * n


engine = TaskQueueEngine(concurrency=3)
engine.register_function("network_call", async_network_call)
engine.register_function("heavy_math", sync_cpu_heavy)


def submit_demo_tasks(target: TaskQueueEngine) -> None:
    """A burst of tasks so the dashboard shows queued, running, completed and failed rows."""
    for site in ("api.github.com", "pypi.org", "python.org", "fastapi.tiangolo.com"):
        target.submit(TaskModel(func_name="network_call",
                      args=[f"https://{site}"], priority=10))
    for n in (2, 3, 4, 5, 6):
        target.submit(
            TaskModel(func_name="heavy_math", args=[n], priority=100))
    # The crash task uses max_retries=1 so the dead-letter queue fills quickly.
    target.submit(TaskModel(func_name="heavy_math", args=[
                  13], priority=1, max_retries=1, timeout=5))


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    await engine.start()
    if DEMO_MODE:
        submit_demo_tasks(engine)
    try:
        yield
    finally:
        await engine.stop()


app = FastAPI(title="Task Queue Engine", lifespan=lifespan)


# ==========================================
# 4. API Endpoints
# ==========================================
@app.get("/", include_in_schema=False)
async def dashboard() -> FileResponse:
    return FileResponse(DASHBOARD_PATH)


# The read-only endpoints are plain functions, so FastAPI runs them in a thread pool
# and the database reads never block the event loop. Endpoints that touch the queue
# stay async, because asyncio.Queue is not thread-safe.
@app.get("/api/status")
def get_status() -> dict[str, Any]:
    stats = {status.value: 0 for status in TaskStatus}
    with get_db() as conn:
        for row in conn.execute("SELECT status, COUNT(*) AS count FROM tasks GROUP BY status"):
            stats[row["status"]] = row["count"]
    return {"engine_status": "Running" if engine.is_running else "Stopped", "stats": stats}


@app.get("/api/tasks")
def get_tasks() -> list[dict[str, Any]]:
    """The 50 newest tasks, plus every failed task so the dead-letter list is never cut off."""
    with get_db() as conn:
        rows = conn.execute(
            """
            SELECT * FROM (
                SELECT * FROM tasks WHERE status = 'FAILED' ORDER BY completed_at DESC LIMIT 200
            )
            UNION ALL
            SELECT * FROM (
                SELECT * FROM tasks WHERE status != 'FAILED' ORDER BY created_at DESC LIMIT 50
            )
            """
        ).fetchall()
    return [dict(row) for row in rows]


@app.post("/api/tasks", status_code=201)
async def create_task(request: TaskCreate) -> dict[str, str]:
    if not engine.has_function(request.func_name):
        raise HTTPException(
            status_code=400, detail=f"Unknown function: {request.func_name}")
    return {"id": engine.submit(TaskModel(**request.model_dump()))}


@app.post("/api/retry/{task_id}")
async def retry_task(task_id: str) -> dict[str, str]:
    with get_db() as conn:
        row = conn.execute("SELECT * FROM tasks WHERE id = ?",
                           (task_id,)).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="Task not found")
    if row["status"] != TaskStatus.FAILED.value:
        raise HTTPException(
            status_code=409, detail="Only FAILED tasks can be retried")

    task = row_to_task(row)
    task.status = TaskStatus.PENDING
    task.retries = 0
    task.error = None
    task.started_at = None
    task.completed_at = None
    engine.submit(task)
    return {"message": f"Task {task_id} re-queued"}


if __name__ == "__main__":
    uvicorn.run(
        "task_engine:app",
        host=os.environ.get("HOST", "127.0.0.1"),
        port=int(os.environ.get("PORT", "8000")),
        reload=False,
        access_log=False,
    )
