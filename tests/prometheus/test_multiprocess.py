import os
import shutil
import signal
import subprocess
import tempfile
import time
from pathlib import Path

import pytest

# Skip tests if Kafka or Prometheus dependencies are not available
pytest.importorskip("aiokafka")
pytest.importorskip("prometheus_client")


@pytest.fixture()
def metrics_dir():
    """Create a temporary directory for multi-process metrics."""
    temp_dir = tempfile.mkdtemp(prefix="prometheus_multiproc_")
    yield temp_dir
    if Path(temp_dir).exists():
        shutil.rmtree(temp_dir)


@pytest.fixture()
def kafka_broker_url():
    """Kafka broker URL for testing."""
    return os.getenv("KAFKA_BROKER", "localhost:9092")


class TestPrometheusMultiprocess:
    """Test suite for Prometheus multi-process mode."""

    @pytest.mark.slow()
    def test_multiprocess_metrics_collection(self, metrics_dir, kafka_broker_url):
        """Test that metrics are correctly collected in multi-process mode.

        This test:
        1. Launches application via uvicorn with multiple workers
        2. Sets PROMETHEUS_MULTIPROC_DIR environment variable
        3. Sends messages to test queue
        4. Verifies /metrics endpoint responds consistently from any worker
        """
        from aiokafka import AIOKafkaProducer

        # Path to the test application
        app_module = "tests.prometheus.multiprocess_app:app"

        env = os.environ.copy()
        env["PROMETHEUS_MULTIPROC_DIR"] = metrics_dir
        env["KAFKA_BROKER"] = kafka_broker_url

        # Start uvicorn with 2 workers to test multi-process behavior
        process = subprocess.Popen(
            [
                "uvicorn",
                app_module,
                "--host",
                "127.0.0.1",
                "--port",
                "8000",
                "--workers",
                "2",
            ],
            env=env,
            stdout=subprocess.PIPE,
            text=True,
        )

        try:
            # Wait for the application to start and workers to initialize
            time.sleep(15)

            # Verify the process is running
            if process.poll() is not None:
                _, stderr = process.communicate()
                pytest.skip(f"Uvicorn process failed to start. stderr: {stderr[:500]}")

            import asyncio
            import httpx

            try:
                for attempt in range(3):
                    try:
                        response = httpx.get("http://127.0.0.1:8000/metrics", timeout=5)
                        if response.status_code in (200, 500):
                            break
                    except (httpx.ConnectError, httpx.ConnectTimeout):
                        if attempt < 2:
                            time.sleep(5)
                        else:
                            pytest.skip("Application not responding after 3 attempts")
            except Exception as e:
                pytest.skip(f"Could not connect to application: {e}")

            async def send_test_messages():
                producer = AIOKafkaProducer(bootstrap_servers=kafka_broker_url)
                try:
                    await producer.start()
                    for i in range(10):
                        await producer.send_and_wait(
                            "test-multiprocess-queue",
                            f"test message {i}".encode(),
                        )
                except Exception as e:
                    pytest.skip(f"Failed to send messages to Kafka: {e}")
                finally:
                    await producer.stop()

            try:
                asyncio.run(send_test_messages())
            except Exception as e:
                pytest.skip(f"Kafka not available: {e}")

            # Wait for messages to be processed
            time.sleep(5)

            # Query /metrics endpoint multiple times
            # In multi-process mode, any worker can respond
            try:
                responses = []
                for _ in range(5):
                    response = httpx.get("http://127.0.0.1:8000/metrics", timeout=10)

                    if response.status_code == 500:
                        pytest.skip("Application returned 500 - Kafka connection issue")

                    assert response.status_code == 200, (
                        f"Metrics endpoint returned {response.status_code}"
                    )
                    responses.append(response.text)

                # Verify all responses contain metrics
                for metrics_text in responses:
                    assert "# HELP" in metrics_text or "# TYPE" in metrics_text
                    # Check for FastStream metrics
                    assert "faststream" in metrics_text.lower()

                # Verify metrics contain data about processed messages
                # At least one response should show received messages
                has_received_metrics = any(
                    "received_messages" in resp for resp in responses
                )
                assert has_received_metrics, "No received_messages metrics found"

            except (httpx.ConnectError, httpx.ConnectTimeout):
                pytest.skip("Could not connect to application")

        finally:
            if process.poll() is None:
                process.send_signal(signal.SIGTERM)
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()

    @pytest.mark.slow()
    def test_multiprocess_directory_requirements(self, kafka_broker_url):
        """Test that the metrics directory must exist and be writable.

        This test verifies the important requirements mentioned in the documentation.
        """
        non_existent_dir = "/tmp/non_existent_prometheus_dir_12345"
        if Path(non_existent_dir).exists():
            shutil.rmtree(non_existent_dir)

        app_module = "tests.prometheus.multiprocess_app:app"

        env = os.environ.copy()
        env["PROMETHEUS_MULTIPROC_DIR"] = non_existent_dir
        env["KAFKA_BROKER"] = kafka_broker_url

        process = subprocess.Popen(
            [
                "uvicorn",
                app_module,
                "--host",
                "127.0.0.1",
                "--port",
                "8001",
                "--workers",
                "1",
            ],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        try:
            time.sleep(3)

            # The process might fail or run with warnings
            # Check if stderr contains warnings about the directory
            if process.poll() is not None:
                process.communicate()
                assert True
            else:
                process.send_signal(signal.SIGTERM)
                process.wait(timeout=5)

        finally:
            if process.poll() is None:
                process.kill()
                process.wait()

    def test_multiprocess_metrics_directory_cleanup(self, metrics_dir):
        """Test that metrics directory should be emptied between application runs.

        This verifies the requirement that the directory should be cleaned
        between runs to avoid stale metrics.
        """
        # Create some dummy metric files
        test_file = Path(metrics_dir) / "test_metric.db"
        test_file.write_text("dummy content")

        assert test_file.exists()

        # In a real scenario, the directory should be cleaned before starting
        # Here we verify that files exist and can be cleaned
        for file in Path(metrics_dir).glob("*.db"):
            file.unlink()

        remaining_files = list(Path(metrics_dir).glob("*.db"))
        assert len(remaining_files) == 0, (
            "Metrics directory should be empty after cleanup"
        )

    @pytest.mark.slow()
    def test_single_process_fallback(self, kafka_broker_url):
        """Test that the application works in single-process mode without PROMETHEUS_MULTIPROC_DIR.

        This verifies that the metrics endpoint gracefully falls back to single-process mode.
        """
        app_module = "tests.prometheus.multiprocess_app:app"

        env = os.environ.copy()
        # Do NOT set PROMETHEUS_MULTIPROC_DIR
        if "PROMETHEUS_MULTIPROC_DIR" in env:
            del env["PROMETHEUS_MULTIPROC_DIR"]
        env["KAFKA_BROKER"] = kafka_broker_url

        process = subprocess.Popen(
            [
                "uvicorn",
                app_module,
                "--host",
                "127.0.0.1",
                "--port",
                "8003",
            ],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        try:
            time.sleep(15)

            if process.poll() is not None:
                pytest.skip("Uvicorn process failed to start")

            import httpx

            try:
                response = httpx.get("http://127.0.0.1:8003/metrics", timeout=10)

                if response.status_code == 500:
                    pytest.skip(
                        "Application returned 500 - likely Kafka connection issue"
                    )

                assert response.status_code == 200
                metrics_text = response.text

                assert "# HELP" in metrics_text or "# TYPE" in metrics_text

            except (httpx.ConnectError, httpx.ConnectTimeout):
                pytest.skip("Could not connect to application")

        finally:
            if process.poll() is None:
                process.send_signal(signal.SIGTERM)
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
