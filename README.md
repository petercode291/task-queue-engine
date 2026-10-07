# Task Queue Engine

[![tests](https://github.com/Petercode291/task-queue-engine/actions/workflows/ci.yml/badge.svg)](https://github.com/Petercode291/task-queue-engine/actions/workflows/ci.yml)

A lightweight Python job scheduler with priorities, retries, async workers, SQLite persistence and a live monitoring dashboard.

![Dashboard](docs/dashboard.png)

## Features

- **Priority scheduling:** a lower number runs first. Tasks with the same priority run first-in, first-out.
- **Async workers:** a configurable number of workers run I/O-bound tasks concurrently.
- **Process pool for CPU work:** synchronous, CPU-heavy functions run in a `ProcessPoolExecutor`, so they don't block the event loop or the dashboard.
- **Retries with exponential backoff:** failed tasks retry after 2s, 4s, 8s and so on, up to `max_retries`.
- **Per-task timeouts:** a task that runs too long is failed with a `TimeoutError`.
- **Dead-letter queue:** permanently failed tasks stay visible in the dashboard with a "Retry manually" button.
- **SQLite persistence:** tasks survive restarts. Tasks that were running or waiting when the engine stopped are re-queued on the next start.
- **Live dashboard:** counters and a task table that refresh every second, with an offline indicator.
- **REST API:** submit and retry tasks over HTTP. Interactive docs are at `/docs`.

## Requirements

Python 3.11 or newer.

## Quick start

```bash
git clone https://github.com/Petercode291/task-queue-engine.git
cd task-queue-engine
python -m venv .venv
.venv\Scripts\activate          # Windows (on macOS/Linux: source .venv/bin/activate)
pip install -r requirements.txt
python task_engine.py --demo
```

Open http://127.0.0.1:8000/ for the dashboard. Without `--demo` the engine starts with an empty queue.

The demo queues a burst of network and CPU tasks. One of them (`heavy_math` with argument `13`) is designed to crash and uses `max_retries=1`, so you can see the dead-letter queue and "Retry manually" quickly. Normal tasks default to 3 retries.

## Submitting your own tasks

Register a function in `task_engine.py`, then submit tasks over the API:

```python
engine.register_function("my_task", my_function)   # async def = worker, plain def = process pool
```

```bash
curl -X POST http://127.0.0.1:8000/api/tasks \
  -H "Content-Type: application/json" \
  -d '{"func_name": "heavy_math", "args": [7], "priority": 5, "max_retries": 2, "timeout": 10}'
```

You can also use the Swagger page at http://127.0.0.1:8000/docs.

## API

| Method | Endpoint | Description |
|---|---|---|
| GET | `/` | Live dashboard |
| GET | `/api/status` | Task counts by status |
| GET | `/api/tasks` | The 50 newest tasks, plus every failed task |
| POST | `/api/tasks` | Submit a task (`func_name`, `args`, `priority`, `max_retries`, `timeout`) |
| POST | `/api/retry/{id}` | Re-queue a FAILED task (other statuses return 409) |

The server sets the task id, status and timestamps. Submitted values are validated: `priority` 0-1000, `max_retries` 0-10, `timeout` 1-3600 seconds.

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `TASK_DB` | `tasks.db` | SQLite database path |
| `HOST` | `127.0.0.1` | Address to bind |
| `PORT` | `8000` | Port to listen on |

## Tests

```bash
pip install -r requirements-dev.txt
pytest
```

The tests use a temporary database, so they never touch `tasks.db`. They cover priority order, equal-priority ordering, retries, permanent failure, timeouts, restart recovery, the process pool, and every API endpoint.

## Project structure

```
task-queue-engine/
├── .github/workflows/ci.yml   # runs the tests on every push
├── docs/dashboard.png         # README screenshot
├── dashboard.html             # live dashboard (served at /)
├── task_engine.py             # engine, models, database and API
├── test_engine.py             # test suite
├── conftest.py                # points the tests at a temporary database
├── pytest.ini
├── requirements.txt
├── requirements-dev.txt
├── LICENSE
└── README.md
```

## How it works

1. A task is saved to SQLite and placed in an in-memory priority queue.
2. A worker takes the highest-priority task and marks it RUNNING.
3. Async functions run on the event loop. Plain functions run in a process pool.
4. On success the task is COMPLETED. On failure it is retried with backoff, then moved to FAILED when the retries run out.
5. On startup, the engine re-queues every PENDING, RETRYING and interrupted RUNNING task from the database.

## Limitations

- Runs as a single process with one SQLite file. It is meant for learning and small workloads, not high throughput.
- No authentication. Don't expose it to an untrusted network.
- A timeout stops waiting for a CPU-bound task, but the process-pool worker keeps running it until it finishes.
- Task results are not stored, only the status and error text.
- Task functions are registered in code, not uploaded over the API.

## License

MIT. See [LICENSE](LICENSE).
