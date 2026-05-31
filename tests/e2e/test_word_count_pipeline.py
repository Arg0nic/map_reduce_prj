from concurrent.futures import ThreadPoolExecutor
import os
from pathlib import Path
import subprocess
import time

import httpx
import pytest
from sqlalchemy import create_engine, text


pytestmark = pytest.mark.e2e
PROJECT_ROOT = Path(__file__).resolve().parents[2]


if os.getenv("RUN_E2E") != "1":
    pytest.skip(
        "Set RUN_E2E=1 and start docker compose before running e2e tests.",
        allow_module_level=True,
    )


API_BASE_URL = os.getenv("E2E_API_BASE_URL", "http://localhost:8000")
DATABASE_URL = os.getenv(
    "E2E_DATABASE_URL",
    "postgresql+psycopg://mapreduce:mapreduce_password@localhost:5432/mapreduce",
)
API_READY_TIMEOUT_SECONDS = float(os.getenv("E2E_API_READY_TIMEOUT_SECONDS", "30"))
JOB_TIMEOUT_SECONDS = float(os.getenv("E2E_JOB_TIMEOUT_SECONDS", "90"))
STRESS_JOB_TIMEOUT_SECONDS = float(os.getenv("E2E_STRESS_JOB_TIMEOUT_SECONDS", "1800"))
UPLOAD_TIMEOUT_SECONDS = float(os.getenv("E2E_UPLOAD_TIMEOUT_SECONDS", "600"))
POLL_INTERVAL_SECONDS = float(os.getenv("E2E_POLL_INTERVAL_SECONDS", "1"))
LARGE_FILE_SIZE_BYTES = int(os.getenv("E2E_LARGE_FILE_SIZE_BYTES", str(1024 * 1024 * 1024)))
MANY_JOBS_COUNT = int(os.getenv("E2E_MANY_JOBS_COUNT", "20"))
MANY_JOBS_MAX_WORKERS = int(os.getenv("E2E_MANY_JOBS_MAX_WORKERS", "8"))
RESILIENCE_FILE_SIZE_BYTES = int(
    os.getenv("E2E_RESILIENCE_FILE_SIZE_BYTES", str(256 * 1024 * 1024))
)
RESILIENCE_WORKER_COUNT = int(os.getenv("E2E_RESILIENCE_WORKER_COUNT", "5"))
RESILIENCE_KILL_COUNT = int(os.getenv("E2E_RESILIENCE_KILL_COUNT", "2"))
RESILIENCE_KILL_DELAY_SECONDS = float(os.getenv("E2E_RESILIENCE_KILL_DELAY_SECONDS", "3"))
RESILIENCE_JOB_TIMEOUT_SECONDS = float(os.getenv("E2E_RESILIENCE_JOB_TIMEOUT_SECONDS", "900"))


def require_stress_e2e_enabled() -> None:
    if os.getenv("RUN_STRESS_E2E") != "1":
        pytest.skip("Set RUN_STRESS_E2E=1 to run stress e2e tests.")


def require_resilience_e2e_enabled() -> None:
    if os.getenv("RUN_RESILIENCE_E2E") != "1":
        pytest.skip("Set RUN_RESILIENCE_E2E=1 to run resilience e2e tests.")


def run_compose_command(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["docker", "compose", *args],
        cwd=PROJECT_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )


def running_worker_container_ids() -> list[str]:
    result = run_compose_command("ps", "-q", "--status", "running", "worker")
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


def find_running_worker_container_id(worker_id: str) -> str | None:
    for container_id in running_worker_container_ids():
        if container_id.startswith(worker_id) or worker_id.startswith(container_id):
            return container_id

    result = subprocess.run(
        ["docker", "ps", "-q", "--filter", f"id={worker_id}"],
        cwd=PROJECT_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    matches = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    return matches[0] if matches else None


def scale_workers(worker_count: int) -> None:
    run_compose_command(
        "up",
        "-d",
        "--no-deps",
        "--scale",
        f"worker={worker_count}",
        "worker",
    )


def wait_for_worker_count(worker_count: int, timeout_seconds: float = 60) -> list[str]:
    deadline = time.monotonic() + timeout_seconds

    while time.monotonic() < deadline:
        worker_ids = running_worker_container_ids()
        if len(worker_ids) >= worker_count:
            return worker_ids
        time.sleep(1)

    pytest.fail(f"Expected {worker_count} running workers, found {len(running_worker_container_ids())}.")


def kill_worker_containers(container_ids: list[str]) -> None:
    if container_ids:
        subprocess.run(
            ["docker", "kill", *container_ids],
            cwd=PROJECT_ROOT,
            check=True,
            capture_output=True,
            text=True,
        )


def wait_for_running_task(job_id: str, timeout_seconds: float = 60) -> dict[str, str]:
    engine = create_engine(DATABASE_URL)
    deadline = time.monotonic() + timeout_seconds
    query = text(
        """
        SELECT task_id, type, worker_id
        FROM tasks
        WHERE job_id = :job_id
          AND status = 'running'
          AND worker_id IS NOT NULL
        ORDER BY started_at, task_id
        LIMIT 1
        """
    )

    while time.monotonic() < deadline:
        with engine.connect() as connection:
            row = connection.execute(query, {"job_id": job_id}).mappings().first()
        if row is not None:
            return dict(row)
        time.sleep(POLL_INTERVAL_SECONDS)

    pytest.fail(f"No running task was recorded for job {job_id} in {timeout_seconds:g} seconds.")


def wait_for_api_ready(client: httpx.Client) -> None:
    deadline = time.monotonic() + API_READY_TIMEOUT_SECONDS
    last_error: Exception | None = None

    while time.monotonic() < deadline:
        try:
            response = client.get(f"{API_BASE_URL}/health")
            if response.status_code == 200:
                return
        except httpx.HTTPError as exc:
            last_error = exc

        time.sleep(POLL_INTERVAL_SECONDS)

    message = f"API Gateway did not become ready at {API_BASE_URL}"
    if last_error is not None:
        message = f"{message}: {last_error}"
    pytest.fail(message)


def upload_text_file(filename: str, input_text: bytes) -> str:
    with httpx.Client(timeout=10) as client:
        upload_response = client.post(
            f"{API_BASE_URL}/files",
            files={"file": (filename, input_text, "text/plain")},
        )
        assert upload_response.status_code == 202, upload_response.text
        return upload_response.json()["job_id"]


def upload_file_path(filename: str, file_path: Path) -> str:
    with httpx.Client(timeout=UPLOAD_TIMEOUT_SECONDS) as client:
        with file_path.open("rb") as handle:
            upload_response = client.post(
                f"{API_BASE_URL}/files",
                files={"file": (filename, handle, "text/plain")},
            )

    assert upload_response.status_code == 202, upload_response.text
    return upload_response.json()["job_id"]


def wait_for_job_result(
    client: httpx.Client,
    job_id: str,
    timeout_seconds: float = JOB_TIMEOUT_SECONDS,
) -> dict[str, int]:
    deadline = time.monotonic() + timeout_seconds

    while time.monotonic() < deadline:
        result_response = client.get(f"{API_BASE_URL}/jobs/{job_id}/result")
        assert result_response.status_code == 200, result_response.text

        payload = result_response.json()
        if "result" in payload:
            return payload["result"]

        assert payload == {"message": "Not ready yet"}
        time.sleep(POLL_INTERVAL_SECONDS)

    pytest.fail(f"Job {job_id} did not finish in {timeout_seconds:g} seconds.")


def wait_for_job_result_by_id(
    job_id: str,
    timeout_seconds: float = JOB_TIMEOUT_SECONDS,
) -> dict[str, int]:
    with httpx.Client(timeout=10) as client:
        return wait_for_job_result(client, job_id, timeout_seconds=timeout_seconds)


def write_repeated_line_file(file_path: Path, target_size_bytes: int) -> int:
    line = b"alpha beta beta gamma delta\n"
    lines_per_chunk = 8192
    chunk = line * lines_per_chunk
    bytes_written = 0
    line_count = 0

    with file_path.open("wb") as handle:
        while bytes_written < target_size_bytes:
            remaining_bytes = target_size_bytes - bytes_written
            if remaining_bytes >= len(chunk):
                handle.write(chunk)
                bytes_written += len(chunk)
                line_count += lines_per_chunk
            else:
                remaining_lines = max(1, (remaining_bytes + len(line) - 1) // len(line))
                handle.write(line * remaining_lines)
                bytes_written += len(line) * remaining_lines
                line_count += remaining_lines

    return line_count


def test_word_count_pipeline_e2e() -> None:
    input_text = (
        "Hello, world!\n"
        "hello MapReduce world\n"
        "\u041f\u0440\u0438\u0432\u0435\u0442 \u043c\u0438\u0440 "
        "\u043f\u0440\u0438\u0432\u0435\u0442\n"
    ).encode("utf-8")
    expected_result = {
        "hello": 2,
        "mapreduce": 1,
        "world": 2,
        "\u043c\u0438\u0440": 1,
        "\u043f\u0440\u0438\u0432\u0435\u0442": 2,
    }

    with httpx.Client(timeout=10) as client:
        wait_for_api_ready(client)
        job_id = upload_text_file("e2e_input.txt", input_text)
        result = wait_for_job_result(client, job_id)

    assert result == expected_result


def test_concurrent_jobs_are_isolated_e2e() -> None:
    first_input = (
        "alpha beta alpha\n"
        "shared shared\n"
    ).encode("utf-8")
    second_input = (
        "gamma beta\n"
        "gamma delta delta\n"
    ).encode("utf-8")

    with httpx.Client(timeout=10) as client:
        wait_for_api_ready(client)

        with ThreadPoolExecutor(max_workers=2) as executor:
            first_future = executor.submit(upload_text_file, "first_job.txt", first_input)
            second_future = executor.submit(upload_text_file, "second_job.txt", second_input)
            first_job_id = first_future.result()
            second_job_id = second_future.result()

        first_result = wait_for_job_result(client, first_job_id)
        second_result = wait_for_job_result(client, second_job_id)

    assert first_result == {
        "alpha": 2,
        "beta": 1,
        "shared": 2,
    }
    assert second_result == {
        "beta": 1,
        "delta": 2,
        "gamma": 2,
    }


@pytest.mark.stress_e2e
def test_large_file_word_count_stress_e2e(tmp_path: Path) -> None:
    require_stress_e2e_enabled()

    large_input_path = tmp_path / "large_input.txt"
    line_count = write_repeated_line_file(large_input_path, LARGE_FILE_SIZE_BYTES)
    expected_result = {
        "alpha": line_count,
        "beta": line_count * 2,
        "delta": line_count,
        "gamma": line_count,
    }

    with httpx.Client(timeout=10) as client:
        wait_for_api_ready(client)
        job_id = upload_file_path("large_input.txt", large_input_path)
        result = wait_for_job_result(
            client,
            job_id,
            timeout_seconds=STRESS_JOB_TIMEOUT_SECONDS,
        )

    assert result == expected_result


@pytest.mark.stress_e2e
def test_many_concurrent_jobs_stress_e2e() -> None:
    require_stress_e2e_enabled()

    jobs = []
    for index in range(MANY_JOBS_COUNT):
        unique_word = f"jobword{index}"
        tail_word = f"tail{index}"
        bucket_word = f"bucket{index % 3}"
        input_text = (
            f"{unique_word} common common {bucket_word}\n"
            f"{unique_word} {tail_word}\n"
        ).encode("utf-8")
        expected_result = {
            bucket_word: 1,
            "common": 2,
            tail_word: 1,
            unique_word: 2,
        }
        jobs.append((f"many_jobs_{index}.txt", input_text, expected_result))

    with httpx.Client(timeout=10) as client:
        wait_for_api_ready(client)

    with ThreadPoolExecutor(max_workers=MANY_JOBS_MAX_WORKERS) as executor:
        upload_futures = [
            executor.submit(upload_text_file, filename, input_text)
            for filename, input_text, _expected_result in jobs
        ]
        job_ids = [future.result() for future in upload_futures]

        result_futures = [
            executor.submit(
                wait_for_job_result_by_id,
                job_id,
                STRESS_JOB_TIMEOUT_SECONDS,
            )
            for job_id in job_ids
        ]
        results = [future.result() for future in result_futures]

    for result, (_filename, _input_text, expected_result) in zip(results, jobs):
        assert result == expected_result


@pytest.mark.resilience_e2e
def test_job_completes_after_some_workers_are_killed_e2e(tmp_path: Path) -> None:
    require_resilience_e2e_enabled()

    original_worker_count = max(1, len(running_worker_container_ids()))
    active_worker_count = max(2, RESILIENCE_WORKER_COUNT)
    kill_count = min(RESILIENCE_KILL_COUNT, active_worker_count - 1)

    large_input_path = tmp_path / "worker_crash_input.txt"
    line_count = write_repeated_line_file(large_input_path, RESILIENCE_FILE_SIZE_BYTES)
    expected_result = {
        "alpha": line_count,
        "beta": line_count * 2,
        "delta": line_count,
        "gamma": line_count,
    }

    try:
        scale_workers(active_worker_count)
        worker_ids = wait_for_worker_count(active_worker_count)

        with httpx.Client(timeout=10) as client:
            wait_for_api_ready(client)
            job_id = upload_file_path("worker_crash_input.txt", large_input_path)

            running_task = wait_for_running_task(job_id)
            worker_container_id = find_running_worker_container_id(running_task["worker_id"])
            if worker_container_id is None:
                pytest.fail(
                    "Running task worker_id does not match a running Docker worker container: "
                    f"{running_task}"
                )

            time.sleep(RESILIENCE_KILL_DELAY_SECONDS)
            extra_container_ids = [
                container_id
                for container_id in running_worker_container_ids()
                if container_id != worker_container_id
            ][: max(0, kill_count - 1)]
            kill_worker_containers([worker_container_id, *extra_container_ids])

            result = wait_for_job_result(
                client,
                job_id,
                timeout_seconds=RESILIENCE_JOB_TIMEOUT_SECONDS,
            )

        assert result == expected_result
    finally:
        scale_workers(original_worker_count)
