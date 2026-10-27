"""Submit PySpark job qua Spark Standalone REST API (cluster mode) + chờ kết quả.

WHY REST 6066 (không SparkSubmitOperator): repo chọn submit job qua Spark Standalone
REST API ở spark-master:6066 (xem CLAUDE.md), driver chạy trên spark-worker — Airflow
KHÔNG cần cài spark-submit/Java. Đây là API ẩn của Spark Standalone nên payload khá
khó tính; giữ nguyên các field bắt buộc bên dưới.

Lưu ý: appResource/pyFiles là đường dẫn file:// NẰM TRÊN WORKER (thư mục
/opt/spark/jobs đã mount vào cả master lẫn worker qua compose/spark.yml).
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
    """Submit `job_file` (vd 'job_bronze.py') và chặn tới khi FINISHED, raise nếu FAILED.

    common.py được gửi kèm qua spark.submit.pyFiles để job import được.
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
        raise RuntimeError(f"Submit thất bại cho {job_file}: {body}")
    logger.info("Đã submit %s, submissionId=%s", job_file, sub_id)

    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        time.sleep(poll_seconds)
        st = requests.get(f"{MASTER_REST}/v1/submissions/status/{sub_id}", timeout=30).json()
        state = st.get("driverState")
        logger.info("%s driverState=%s", job_file, state)
        if state == "FINISHED":
            return
        if state in {"FAILED", "ERROR", "KILLED"}:
            raise RuntimeError(f"Job {job_file} kết thúc trạng thái {state}: {st}")
    raise TimeoutError(f"Job {job_file} quá {timeout_seconds}s chưa xong.")
