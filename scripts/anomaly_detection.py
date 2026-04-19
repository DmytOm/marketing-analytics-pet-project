import os
import json
import psycopg2
import anthropic
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    'host': os.getenv("DB_HOST"),
    'port': int(os.getenv("DB_PORT")),
    'dbname': os.getenv("DB_NAME"),
    'user': os.getenv("DB_USER"),
    'password': os.getenv("DB_PASSWORD"),
}

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")


def get_marketing_data() -> list[dict]:
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    cursor.execute('''
                   SELECT channel,
                          month,
                          total_spend,
                          total_revenue,
                          total_orders,
                          roas,
                          cac
                   FROM public_marts.mart_marketing_performance
                   ORDER BY month DESC, channel
                   LIMIT 30
                   ''')

    columns = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    return [dict(zip(columns, row)) for row in rows]

def detect_anomalies(data: list[dict]) -> str:
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    data_str = json.dumps(data, indent=2, default=str)

    message = client.messages.create(
        model='claude-opus-4-5',
        max_tokens=1024,
        messages=[
            {
                'role': 'user',
                'content': f'''You are a marketing analytics expert. 
Analyze the following marketing performance data and identify any anomalies or concerning trends.

Data from mart_marketing_performance (last 30 rows, ordered by month desc):
{data_str}

Please:
1. Identify any anomalies in ROAS, CAC, spend, or revenue
2. Highlight channels that are underperforming
3. Note any unusual month-over-month changes
4. Provide a brief summary with actionable insights

Keep your response concise and focused on the most important findings.'''
            }
        ]
    )

    return message.content[0].text


def main():
    print('📊 Fetching marketing data...')
    data = get_marketing_data()
    print(f'✅ Got {len(data)} rows')

    print('🤖 Analyzing with Claude...')
    analysis = detect_anomalies(data)

    print('\n' + '='*50)
    print('ANOMALY DETECTION REPORT')
    print('='*50)
    print(analysis)
    print('='*50)

if __name__ == '__main__':
    main()

