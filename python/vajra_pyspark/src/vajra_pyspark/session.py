"""VajraSession — convenience wrapper around PySpark SparkSession for Vajra."""
from __future__ import annotations

import contextlib
import os
from typing import Optional

from pyspark.sql import SparkSession


class VajraSession:
    """Factory for PySpark SparkSession instances pointing at a Vajra server."""

    @staticmethod
    def connect(
        host: str = "localhost",
        port: int = 50051,
        *,
        token: Optional[str] = None,
        app_name: str = "vajra",
    ) -> SparkSession:
        """Return a SparkSession connected to a running Vajra server.

        Args:
            host: Vajra server hostname or IP (default: localhost).
            port: Vajra server gRPC port (default: 50051).
            token: Optional Bearer token for auth (VAJRA_AUTH_TOKEN env var is also read).
            app_name: Application name shown in logs.
        """
        auth_token = token or os.environ.get("VAJRA_AUTH_TOKEN")
        remote_url = f"sc://{host}:{port}"
        if auth_token:
            remote_url += f"/;token={auth_token}"
        builder = SparkSession.builder.remote(remote_url).appName(app_name)
        return builder.getOrCreate()

    @staticmethod
    @contextlib.contextmanager
    def local(
        port: int = 50051,
        *,
        mode: str = "local",
        workers: int = 4,
        token: Optional[str] = None,
        vajra_bin: Optional[str] = None,
    ):
        """Context manager that starts a local Vajra server and yields a SparkSession.

        Requires `vajra` binary in PATH or VAJRA_BIN environment variable.

        Usage::

            with VajraSession.local() as spark:
                spark.sql("SELECT 1+1").show()
        """
        import subprocess
        import time

        bin_path = vajra_bin or os.environ.get("VAJRA_BIN", "vajra")
        env = {**os.environ}
        if mode == "local-cluster":
            env["SAIL_MODE"] = "local-cluster"
            env["SAIL_CLUSTER__WORKER_INITIAL_COUNT"] = str(workers)
        else:
            env["SAIL_MODE"] = "local"

        cmd = [bin_path, "server", "--ip", "0.0.0.0", "--port", str(port)]
        if token:
            cmd += ["--auth-token", token]

        proc = subprocess.Popen(cmd, env=env)
        try:
            # Wait up to 10 s for server to be ready
            import socket
            for _ in range(40):
                try:
                    with socket.create_connection(("localhost", port), timeout=0.25):
                        break
                except OSError:
                    time.sleep(0.25)
            else:
                raise RuntimeError(f"Vajra server did not start on port {port} within 10s")

            spark = VajraSession.connect(port=port, token=token)
            try:
                yield spark
            finally:
                spark.stop()
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
