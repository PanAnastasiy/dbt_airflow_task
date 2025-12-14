import os
import requests
from loguru import logger


class TelegramAlert:

    def __init__(self):
        self.token = os.getenv("TG_BOT_TOKEN")
        self.chat_id = os.getenv("TG_CHAT_ID")
        self.base_url = f"https://api.telegram.org/bot{self.token}/sendMessage"

    def send(self, context):

        if not self.token or not self.chat_id:
            logger.warning(
                "Telegram notification skipped: TG_BOT_TOKEN or TG_CHAT_ID not found"
            )
            return

        try:
            dag_id = context["dag"].dag_id
            task_instance = context["task_instance"]

            task_id = task_instance.task_id
            status = task_instance.state
            execution_date = context.get("execution_date")
            exception = context.get("exception")

            emoji = "✅" if status == "success" else "❌"
            error_msg = f"\nError: {exception}" if exception else ""

            msg = (
                f"{emoji} <b>Airflow Alert</b>\n"
                f"<b>DAG:</b> <code>{dag_id}</code>\n"
                f"<b>Task:</b> <code>{task_id}</code>\n"
                f"<b>Status:</b> {status}\n"
                f"<b>Time:</b> {execution_date}"
                f"{error_msg}"
            )

            payload = {
                "chat_id": self.chat_id,
                "text": msg,
                "parse_mode": "HTML",
            }

            response = requests.post(self.base_url, data=payload, timeout=10)
            response.raise_for_status()

            logger.info("Telegram notification sent for task={}", task_id)

        except Exception as e:
            logger.exception("Failed to send Telegram alert")
