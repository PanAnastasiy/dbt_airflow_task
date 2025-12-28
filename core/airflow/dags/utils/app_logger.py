import sys
from typing import Any

from airflow.models import TaskInstance
from loguru import logger


class AppLogger:

    def __init__(self, log_file: str):
        logger.remove()

        console_format = (
            "<green>{time:YYYY-MM-DD HH:mm:ss}</green> "
            "<level>[{level}]</level> "
            "<magenta>{module}</magenta>::"
            "<cyan>{function}</cyan> "
            "<yellow>({line})</yellow> → "
            "<level>{message}</level>"
        )

        file_format = (
            "{time:YYYY-MM-DD HH:mm:ss} | "
            "{level} | "
            "{module}.{function}:{line} | "
            "{message}"
        )

        logger.add(
            sink=sys.stdout,
            level="INFO",
            format=console_format,
            enqueue=True,
        )

        logger.add(
            sink=log_file,
            level="DEBUG",
            format=file_format,
            rotation="10 MB",
            retention="10 days",
            compression="zip",
            serialize=True,
            enqueue=True,
        )

        self._logger = logger.bind(
            app="airflow",
            component="dag",
        )

    def info(self, message: str, **extra: Any) -> None:
        self._logger.bind(**extra).info(message)

    def success(self, message: str, **extra: Any) -> None:
        self._logger.bind(**extra).success(message)

    def warning(self, message: str, **extra: Any) -> None:
        self._logger.bind(**extra).warning(message)

    def error(self, message: str, **extra: Any) -> None:
        self._logger.bind(**extra).error(message)

    def exception(self, message: str, **extra: Any) -> None:
        self._logger.bind(**extra).exception(message)

    def task_start(self, context: dict[str, Any]) -> None:
        ti: TaskInstance = context["task_instance"]
        self.info(
            "Task started",
            dag_id=ti.dag_id,
            task_id=ti.task_id,
            run_id=ti.run_id,
        )

    def task_success(self, context: dict[str, Any]) -> None:
        ti: TaskInstance = context["task_instance"]
        self.success(
            "Task finished successfully",
            dag_id=ti.dag_id,
            task_id=ti.task_id,
            duration=ti.duration,
        )

    def task_failure(self, context: dict[str, Any]) -> None:
        ti: TaskInstance = context["task_instance"]
        self.error(
            "Task failed",
            dag_id=ti.dag_id,
            task_id=ti.task_id,
            error=str(context.get("exception")),
        )

    def dag_success(self, context: dict[str, Any]) -> None:
        dag_run = context.get("dag_run")
        self.success(
            "DAG completed successfully",
            dag_id=dag_run.dag_id if dag_run else "unknown",
            run_id=dag_run.run_id if dag_run else None,
        )
