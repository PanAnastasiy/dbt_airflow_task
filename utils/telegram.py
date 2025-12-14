import os
import requests

class TelegramAlert:

    def __init__(self):
        self.token = os.getenv('TG_BOT_TOKEN')
        self.chat_id = os.getenv('TG_CHAT_ID')
        self.base_url = f"https://api.telegram.org/bot{self.token}/sendMessage"

    def send(self, context):

        if not self.token or not self.chat_id:
            print("Telegram notification skipped: TG_BOT_TOKEN or TG_CHAT_ID not found in env.")
            return

        try:

            dag_id = context.get('dag').dag_id
            task_instance = context.get('task_instance')
            task_id = task_instance.task_id
            status = task_instance.state
            execution_date = context.get('execution_date')
            exception = context.get('exception')
            error_msg = f"\nError: {exception}" if exception else ""
            emoji = "✅" if status == "success" else "❌"

            msg = (
                f"{emoji} **Airflow Alert**\n"
                f"**DAG:** `{dag_id}`\n"
                f"**Task:** `{task_id}`\n"
                f"**Status:** {status}\n"
                f"**Time:** {execution_date}"
                f"{error_msg}"
            )

            payload = {
                'chat_id': self.chat_id,
                'text': msg,
                'parse_mode': 'Markdown'
            }

            response = requests.post(self.base_url, data=payload, timeout=10)
            response.raise_for_status()
            print(f"Telegram notification sent for {task_id}")

        except Exception as e:
            print(f"Failed to send Telegram alert: {e}")