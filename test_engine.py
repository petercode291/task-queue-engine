import asyncio
import time
from collections.abc import AsyncGenerator, Callable, Iterator

import pytest
import pytest_asyncio
from fastapi.testclient import TestClient

import task_engine
from task_engine import (
    TaskModel,
    TaskQueueEngine,
    TaskStatus,
    app,
    get_db,
    row_to_task,
)


# ------------------------------------------------------------------ helpers
def read_task(task_id: str):
    with get_db() as conn:
        return conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()


async def wait_for_status(task_id: str, status: TaskStatus, timeout: float = 5.0) -> None:
    """Poll the database until the task reaches a status (faster and less flaky than sleeping)."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        row = read_task(task_id)
        if row is not None and row["status"] == status.value:
            return
        await asyncio.sleep(0.05)
    row = read_task(task_id)
    raise AssertionError(
        f"task {task_id} is {row['status'] if row else 'missing'}, expected {status.value}")


def wait_until(check: Callable[[], bool], timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if check():
            return
        time.sleep(0.05)
    raise AssertionError("condition not met in time")


# ------------------------------------------------------------------ engine tests
@pytest_asyncio.fixture
async def engine() -> AsyncGenerator[TaskQueueEngine, None]:
    eng = TaskQueueEngine(concurrency=1)
    eng.backoff_multiplier = 0.1  # retries take fractions of a second in tests

    async def mock_success() -> bool:
        return True

    async def mock_fail() -> None:
        raise ValueError("Intentional Failure")

    async def mock_timeout() -> None:
        await asyncio.sleep(2)

    eng.register_function("success", mock_success)
    eng.register_function("fail", mock_fail)
    eng.register_function("timeout_func", mock_timeout)

    await eng.start()
    yield eng
    await eng.stop()


@pytest.mark.asyncio
async def test_successful_task_completes(engine: TaskQueueEngine) -> None:
    task = TaskModel(func_name="success")
    engine.submit(task)
    await wait_for_status(task.id, TaskStatus.COMPLETED)

    saved = read_task(task.id)
    assert saved["started_at"] is not None
    assert saved["completed_at"] >= saved["started_at"]


@pytest.mark.asyncio
async def test_priority_order(engine: TaskQueueEngine) -> None:
    order: list[str] = []

    async def track_exec(name: str) -> None:
        order.append(name)

    engine.register_function("track", track_exec)
    low = TaskModel(func_name="track", args=["low_priority"], priority=100)
    high = TaskModel(func_name="track", args=["high_priority"], priority=1)
    engine.submit(low)
    engine.submit(high)

    await wait_for_status(low.id, TaskStatus.COMPLETED)
    await wait_for_status(high.id, TaskStatus.COMPLETED)
    assert order == ["high_priority", "low_priority"]


@pytest.mark.asyncio
async def test_priority_tie_runs_first_in_first_out(engine: TaskQueueEngine) -> None:
    order: list[str] = []

    async def track_exec(name: str) -> None:
        order.append(name)

    engine.register_function("track", track_exec)
    first = TaskModel(func_name="track", args=["first_tie"], priority=10)
    second = TaskModel(func_name="track", args=["second_tie"], priority=10)
    engine.submit(first)
    engine.submit(second)

    await wait_for_status(second.id, TaskStatus.COMPLETED)
    assert order == ["first_tie", "second_tie"]


@pytest.mark.asyncio
async def test_retry_count_and_permanent_failure(engine: TaskQueueEngine) -> None:
    task = TaskModel(func_name="fail", max_retries=2, priority=1)
    engine.submit(task)
    await wait_for_status(task.id, TaskStatus.FAILED)

    saved = read_task(task.id)
    assert saved["retries"] == 2
    assert "Intentional Failure" in saved["error"]


@pytest.mark.asyncio
async def test_timeout_enforcement(engine: TaskQueueEngine) -> None:
    task = TaskModel(func_name="timeout_func", timeout=1, max_retries=0)
    engine.submit(task)
    await wait_for_status(task.id, TaskStatus.FAILED, timeout=4)

    assert "TimeoutError" in read_task(task.id)["error"]


@pytest.mark.asyncio
async def test_unknown_function_is_rejected(engine: TaskQueueEngine) -> None:
    with pytest.raises(ValueError):
        engine.submit(TaskModel(func_name="does_not_exist"))


@pytest.mark.asyncio
async def test_restart_requeues_running_task(engine: TaskQueueEngine) -> None:
    ran: list[int] = []

    async def record() -> None:
        ran.append(1)

    stuck = TaskModel(func_name="success", status=TaskStatus.RUNNING)
    engine._save_task(stuck)  # simulate a crash while the task was running
    await engine.stop()

    second_engine = TaskQueueEngine(concurrency=1)
    second_engine.register_function("success", record)
    await second_engine.start()
    await wait_for_status(stuck.id, TaskStatus.COMPLETED)
    await second_engine.stop()
    assert ran == [1]


@pytest.mark.asyncio
async def test_cpu_task_runs_in_process_pool() -> None:
    eng = TaskQueueEngine(concurrency=1)
    eng.register_function("heavy_math", task_engine.sync_cpu_heavy)
    await eng.start()
    task = TaskModel(func_name="heavy_math", args=[4])
    eng.submit(task)
    await wait_for_status(task.id, TaskStatus.COMPLETED, timeout=10)
    await eng.stop()


def test_row_to_task_restores_args() -> None:
    task_engine.init_db()
    original = TaskModel(func_name="success", args=["a", 2, {"k": "v"}])
    TaskQueueEngine()._save_task(original)

    restored = row_to_task(read_task(original.id))
    assert restored.args == ["a", 2, {"k": "v"}]
    assert restored.id == original.id


# ------------------------------------------------------------------ API tests
@pytest.fixture
def client() -> Iterator[TestClient]:
    with TestClient(app) as test_client:
        yield test_client


def insert_task(status: TaskStatus, func_name: str = "network_call") -> TaskModel:
    task = TaskModel(func_name=func_name, args=[
                     "https://example.com"], status=status)
    task_engine.engine._save_task(task)
    return task


def test_dashboard_is_served(client: TestClient) -> None:
    response = client.get("/")
    assert response.status_code == 200
    assert "Engine Status" in response.text


def test_create_task_runs_it(client: TestClient) -> None:
    response = client.post(
        "/api/tasks", json={"func_name": "network_call", "args": ["https://example.com"]})
    assert response.status_code == 201
    task_id = response.json()["id"]
    wait_until(lambda: read_task(task_id)[
               "status"] == TaskStatus.COMPLETED.value, timeout=6)


def test_create_task_rejects_unknown_function(client: TestClient) -> None:
    response = client.post("/api/tasks", json={"func_name": "nope"})
    assert response.status_code == 400


def test_create_task_rejects_client_chosen_fields(client: TestClient) -> None:
    response = client.post(
        "/api/tasks", json={"func_name": "network_call", "id": "x');alert(1);//"})
    assert response.status_code == 422


@pytest.mark.parametrize("payload", [{"timeout": -5}, {"max_retries": 100000}, {"priority": -1}])
def test_create_task_validates_limits(client: TestClient, payload: dict) -> None:
    response = client.post(
        "/api/tasks", json={"func_name": "network_call", **payload})
    assert response.status_code == 422


def test_retry_unknown_task_returns_404(client: TestClient) -> None:
    assert client.post("/api/retry/missing").status_code == 404


def test_retry_only_allowed_for_failed_tasks(client: TestClient) -> None:
    completed = insert_task(TaskStatus.COMPLETED)
    assert client.post(f"/api/retry/{completed.id}").status_code == 409


def test_retry_requeues_failed_task(client: TestClient) -> None:
    failed = insert_task(TaskStatus.FAILED)
    response = client.post(f"/api/retry/{failed.id}")
    assert response.status_code == 200
    wait_until(lambda: read_task(failed.id)[
               "status"] == TaskStatus.COMPLETED.value, timeout=6)


def test_status_counts_every_state(client: TestClient) -> None:
    insert_task(TaskStatus.COMPLETED)
    insert_task(TaskStatus.COMPLETED)
    insert_task(TaskStatus.FAILED)

    stats = client.get("/api/status").json()["stats"]
    assert stats["COMPLETED"] == 2
    assert stats["FAILED"] == 1
    assert stats["RUNNING"] == 0


def test_task_list_keeps_all_failed_and_limits_the_rest(client: TestClient) -> None:
    for _ in range(60):
        insert_task(TaskStatus.COMPLETED)
    for _ in range(3):
        insert_task(TaskStatus.FAILED)

    tasks = client.get("/api/tasks").json()
    assert sum(t["status"] == "FAILED" for t in tasks) == 3
    assert sum(t["status"] == "COMPLETED" for t in tasks) == 50
