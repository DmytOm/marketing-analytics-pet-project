import os
import sys
import json
import requests
from dotenv import load_dotenv


load_dotenv()

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

def send_slack_notification(message:str, is_error: bool = False) -> None:
    emoji = '🚨' if is_error else '✅'
    payload = {
        'text': f'{emoji} *dbt Pipeline Alert*\n{message}'
    }

    response = requests.post(
        SLACK_WEBHOOK_URL,
        data=json.dumps(payload),
        headers={'Content-Type': 'application/json'}
    )

    if response.status_code != 200:
        print(f'Failed to send Slack message: {response.text}')


def notify_success() -> None:
    send_slack_notification('Pipeline Completed successfully! All tests passed.')


def notify_failure(error_message: str) -> None:
    send_slack_notification(f'Pipeline Failed.\n```{error_message}```',is_error = True)


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Usage: python notify_slack.py [success|failure] [error_message]')
        sys.exit(1)

    status = sys.argv[1]

    if status == 'success':
        notify_success()
    elif status == 'failure':
        error_msg = sys.argv[2] if len(sys.argv) > 2 else 'Unknown error'
        notify_failure(error_msg)