import os
import anthropic
from dotenv import load_dotenv

load_dotenv()

ANTHROPIC_API_KEY = os.getenv('ANTHROPIC_API_KEY')

MODELS_CONTEXT = {
    'stg_customers': {
        'description': 'Staging model for customers',
        'columns': ['customer_id', 'first_name', 'last_name', 'email',
                   'country', 'city', 'age', 'gender', 'created_date', 'created_at']
    },
    'stg_orders': {
        'description': 'Staging model for orders',
        'columns': ['order_id', 'customer_id', 'session_id', 'status',
                   'amount', 'ordered_at', 'ordered_date']
    },
    'mart_customer_orders': {
        'description': 'Customer orders mart with LTV metrics',
        'columns': ['customer_id', 'first_name', 'last_name', 'email',
                   'country', 'gender', 'age', 'created_date', 'total_orders',
                   'lifetime_value', 'avg_order_value', 'first_order_date',
                   'last_order_date', 'completed_orders', 'refunded_orders']
    },
    'mart_marketing_performance': {
        'description': 'Marketing performance by channel and month',
        'columns': ['channel', 'month', 'total_spend', 'total_impressions',
                   'total_clicks', 'total_sessions', 'total_orders',
                   'total_revenue', 'roas', 'cac']
    },
}


def generate_model_docs(model_name: str, model_info: dict) -> str:
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    message = client.messages.create(
        model='claude-opus-4-5',
        max_tokens=1024,
        messages=[
            {
                'role': 'user',
                'content': f'''You are a dbt documentation expert.
Generate YAML documentation for the following dbt model.

Model name: {model_name}
Current description: {model_info['description']}
Columns: {', '.join(model_info['columns'])}

Generate a dbt YAML documentation block with:
- A clear model description (2-3 sentences)
- Description for each column

Format as valid dbt YAML. Use this structure:
  - name: {model_name}
    description: "..."
    columns:
      - name: column_name
        description: "..."

Return ONLY the YAML, no explanations.'''
            }
        ]
    )

    return message.content[0].text


def main() -> None:
    print('📝 Generating dbt documentation...\n')

    for model_name, model_info in MODELS_CONTEXT.items():
        print(f'Generating docs for {model_name}...')
        docs = generate_model_docs(model_name, model_info)
        print(docs)
        print()


if __name__ == '__main__':
    main()