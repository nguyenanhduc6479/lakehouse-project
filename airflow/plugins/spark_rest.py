"""Submit a PySpark job via the Spark Standalone REST API (cluster mode) + wait for the result.

WHY REST 6066 (not SparkSubmitOperator): the repo submits jobs through the Spark Standalone
REST API at spark-master:6066 (see CLAUDE.md) and the driver runs on spark-worker — Airflow
does NOT need spark-submit/Java installed. This is a hidden Spark Standalone API, so the
payload is picky; keep the required fields below as they are.

Note: appResource/pyFiles are file:// paths ON THE WORKER (the /opt/spark/jobs
directory is mounted into both master and worker via compose/spark.yml).
"""

from __future__ import annotations

import logging
import time

import requests

logger = logging.getLogger(__name__)

MASTER_REST = "http://spark-master:6066"
SPARK_MASTER = "spark://spark-master:7077"
JOBS_DIR = "/opt/spark/jobs"
SPARK_VERSION = "3.5.1"


def submit_and_wait(job_file: str, *, poll_seconds: int = 10, timeout_seconds: int = 1800) -> None:
    """Submit `job_file` (e.g. 'job_bronze.py') and block until FINISHED; raise if FAILED.

    common.py is shipped along via spark.submit.pyFiles so the job can import it.
    """
    app_resource = f"file:{JOBS_DIR}/{job_file}"
    payload = {
        "action": "CreateSubmissionRequest",
        "appResource": app_resource,
        "clientSparkVersion": SPARK_VERSION,
        "mainClass": "org.apache.spark.deploy.SparkSubmit",
        "appArgs": [app_resource],
        "environmentVariables": {"SPARK_ENV_LOADED": "1"},
        "sparkProperties": {
            "spark.master": SPARK_MASTER,
            "spark.submit.deployMode": "cluster",
            "spark.app.name": job_file,
            "spark.submit.pyFiles": f"{JOBS_DIR}/common.py",
            "spark.driver.supervise": "false",
        },
    }

    resp = requests.post(f"{MASTER_REST}/v1/submissions/create", json=payload, timeout=30)
    resp.raise_for_status()
    body = resp.json()
    sub_id = body.get("submissionId")
    if not body.get("success") or not sub_id:
        raise RuntimeError(f"Submit failed for {job_file}: {body}")
    logger.info("Submitted %s, submissionId=%s", job_file, sub_id)

    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        time.sleep(poll_seconds)
        st = requests.get(f"{MASTER_REST}/v1/submissions/status/{sub_id}", timeout=30).json()
        state = st.get("driverState")
        logger.info("%s driverState=%s", job_file, state)
        if state == "FINISHED":
            return
        if state in {"FAILED", "ERROR", "KILLED"}:
            raise RuntimeError(f"Job {job_file} ended in state {state}: {st}")
    raise TimeoutError(f"Job {job_file} did not finish within {timeout_seconds}s.")
