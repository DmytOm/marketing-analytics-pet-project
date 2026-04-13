import os
import random
from datetime import date, timedelta

import pandas as pd
import psycopg2
from dotenv import load_dotenv
from faker import Faker

load_dotenv()

RANDOM_SEED = 42
NUM_CUSTOMERS = 1000
NUM_SESSIONS = 5000
NUM_ORDERS = 2000
NUM_EMAIL_EVENTS = 8000
START_DATE = date.today() - timedelta(days=360)
END_DATE = date.today()

DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'port': int(os.getenv('DB_PORT')),
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
}

fake = Faker()
Faker.seed(RANDOM_SEED)
random.seed(RANDOM_SEED)


def generate_customers(n: int) -> pd.DataFrame:
    countries = ['Ukraine', 'Poland', 'Germany', 'USA', 'UK']
    customers = []
    for i in range(1, n + 1):
        customers.append({
            'customer_id': i,
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'email': fake.unique.email(),
            'country': random.choice(countries),
            'city': fake.city(),
            'age': random.randint(18, 65),
            'gender': random.choice(['Male', 'Female']),
            'created_at': fake.date_time_between(
                start_date=START_DATE,
                end_date=END_DATE,
            ),
        })

    return pd.DataFrame(customers)


def generate_sessions(n: int, customers_ids: list) -> pd.DataFrame:
    countries = ['Ukraine', 'Poland', 'Germany', 'USA', 'UK']
    channels = ['google', 'facebook', 'instagram', 'email', 'organic', 'direct']
    devices = ['mobile', 'desktop', 'tablet']
    sessions = []

    for i in range(1, n + 1):
        sessions.append({
            'session_id': i,
            'customer_id': random.choice(customers_ids),
            'channel': random.choice(channels),
            'started_at': fake.date_time_between(
                start_date=START_DATE,
                end_date=END_DATE,
            ),
            'duration_seconds': random.randint(10, 1800),
            'pages_viewed': random.randint(1, 20),
            'device': random.choice(devices),
            'country': random.choice(countries),
        })
    return pd.DataFrame(sessions)


def generate_orders(n: int, customer_ids: list, session_ids: list) -> pd.DataFrame:
    statuses = ['completed', 'pending', 'refunded', 'cancelled']
    orders = []

    for i in range(1, n + 1):
        orders.append({
            'order_id': i,
            'customer_id': random.choice(customer_ids),
            'session_id': random.choice(session_ids),
            'status': random.choices(
                statuses,
                weights=[70, 15, 10, 5],
                k=1
            )[0],
            'amount': round(random.uniform(5.0, 500.0), 2),
            'ordered_at': fake.date_time_between(
                start_date=START_DATE,
                end_date=END_DATE,
            )
        })

    return pd.DataFrame(orders)


def generate_ad_spend(start_date: date, end_date: date) -> pd.DataFrame:
    channels = ['google', 'facebook', 'instagram']
    ad_spends = []

    current_date = start_date
    while current_date <= end_date:
        for channel in channels:
            ad_spends.append({
                'date': current_date,
                'channel': channel,
                'spend': round(random.uniform(5.0, 500.0), 2),
                'impressions': random.randint(1000, 50000),
                'clicks': random.randint(500, 2000),
            })
        current_date += timedelta(days=1)

    return pd.DataFrame(ad_spends)


def generate_email_events(n: int, customer_ids: list) -> pd.DataFrame:
    campaigns = [
        'welcome_series',
        'black_friday',
        'winter_sale',
        'reactivation',
        'newsletter',
    ]

    email_events = []

    for i in range(1, n + 1):
        sent_at = fake.date_time_between(
            start_date=START_DATE,
            end_date=END_DATE,
        )
        is_opened = random.choices([True, False], weights=[40, 60], k=1)[0]
        is_clicked = random.choices([True, False], weights=[20, 80], k=1)[0] if is_opened else False

        email_events.append({
            'event_id': i,
            'customer_id': random.choice(customer_ids),
            'campaign': random.choice(campaigns),
            'sent_at': sent_at,
            'is_opened': is_opened,
            'is_clicked': is_clicked,
        })

    return pd.DataFrame(email_events)


def load_to_postgres(df: pd.DataFrame, table_name: str, conn) -> None:
    cursor = conn.cursor()

    columns = ', '.join(df.columns)
    placeholders = ', '.join(['%s'] * len(df.columns))
    insert_query = f'INSERT INTO {table_name} ({columns}) VALUES ({placeholders})'
    data = [tuple(row) for row in df.itertuples(index=False)]
    cursor.executemany(insert_query, data)
    conn.commit()

    print(f'✅ Loaded {len(df)} rows into {table_name}')
    cursor.close()


def main() -> None:
    print('Starting date generation...')

    customers_df = generate_customers(NUM_CUSTOMERS)
    sessions_df = generate_sessions(NUM_SESSIONS, customers_df['customer_id'].tolist())
    orders_df = generate_orders(NUM_ORDERS, customers_df['customer_id'].tolist(), sessions_df['session_id'].tolist())
    ad_spend_df = generate_ad_spend(START_DATE, END_DATE)
    email_events_df = generate_email_events(NUM_EMAIL_EVENTS, customers_df['customer_id'].tolist())

    print('Data generation complete. Loading to PostgreSQL...')

    conn = psycopg2.connect(**DB_CONFIG)

    try:
        load_to_postgres(customers_df, 'customers', conn)
        load_to_postgres(sessions_df, 'sessions', conn)
        load_to_postgres(orders_df, 'orders', conn)
        load_to_postgres(ad_spend_df, 'ad_spend', conn)
        load_to_postgres(email_events_df, 'email_events', conn)

    finally:
        conn.close()
        print('Done! Connection closed.')


if __name__ == '__main__':
    main()
