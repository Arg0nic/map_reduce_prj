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


def test_word_count_pipeline_e2e() -> None:
    input_text = (
        "Hello, world!\n"
        "hello MapReduce world\n"
        "Привет мир привет\n"
    ).encode("utf-8")
    expected_result = {
        "hello": 2,
        "mapreduce": 1,
        "world": 2,
        "мир": 1,
        "привет": 2,
    }

    with httpx.Client(timeout=10) as client:
        wait_for_api_ready(client)

        upload_response = client.post(
            f"{API_BASE_URL}/files",
            files={"file": ("e2e_input.txt", input_text, "text/plain")},
        )
        assert upload_response.status_code == 202, upload_response.text

        job_id = upload_response.json()["job_id"]
        deadline = time.monotonic() + JOB_TIMEOUT_SECONDS

        while time.monotonic() < deadline:
            result_response = client.get(f"{API_BASE_URL}/jobs/{job_id}/result")
            assert result_response.status_code == 200, result_response.text

            payload = result_response.json()
            if "result" in payload:
                assert payload["result"] == expected_result
                return

            assert payload == {"message": "Not ready yet"}
            time.sleep(POLL_INTERVAL_SECONDS)

    pytest.fail(f"Job {job_id} did not finish in {JOB_TIMEOUT_SECONDS:g} seconds.")
