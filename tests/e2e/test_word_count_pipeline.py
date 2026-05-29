from concurrent.futures import ThreadPoolExecutor
import os
import time

import httpx
import pytest


pytestmark = pytest.mark.e2e


if os.getenv("RUN_E2E") != "1":
    pytest.skip(
        "Set RUN_E2E=1 and start docker compose before running e2e tests.",
        allow_module_level=True,
    )


API_BASE_URL = os.getenv("E2E_API_BASE_URL", "http://localhost:8000")
API_READY_TIMEOUT_SECONDS = float(os.getenv("E2E_API_READY_TIMEOUT_SECONDS", "30"))
JOB_TIMEOUT_SECONDS = float(os.getenv("E2E_JOB_TIMEOUT_SECONDS", "90"))
POLL_INTERVAL_SECONDS = float(os.getenv("E2E_POLL_INTERVAL_SECONDS", "1"))


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


def wait_for_job_result(client: httpx.Client, job_id: str) -> dict[str, int]:
    deadline = time.monotonic() + JOB_TIMEOUT_SECONDS

    while time.monotonic() < deadline:
        result_response = client.get(f"{API_BASE_URL}/jobs/{job_id}/result")
        assert result_response.status_code == 200, result_response.text

        payload = result_response.json()
        if "result" in payload:
            return payload["result"]

        assert payload == {"message": "Not ready yet"}
        time.sleep(POLL_INTERVAL_SECONDS)

    pytest.fail(f"Job {job_id} did not finish in {JOB_TIMEOUT_SECONDS:g} seconds.")


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
