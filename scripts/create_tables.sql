CREATE TABLE IF NOT EXISTS customers (
    customer_id   INTEGER PRIMARY KEY,
    first_name    VARCHAR(100),
    last_name     VARCHAR(100),
    email         VARCHAR(255) UNIQUE,
    country       VARCHAR(100),
    city          VARCHAR(100),
    age           INTEGER,
    gender        VARCHAR(10),
    created_at    TIMESTAMP
);

CREATE TABLE IF NOT EXISTS sessions (
    session_id       INTEGER PRIMARY KEY,
    customer_id      INTEGER REFERENCES customers(customer_id),
    channel          VARCHAR(50),
    started_at       TIMESTAMP,
    duration_seconds INTEGER,
    pages_viewed     INTEGER,
    device           VARCHAR(20),
    country          VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS orders (
    order_id    INTEGER PRIMARY KEY,
    customer_id INTEGER REFERENCES customers(customer_id),
    session_id  INTEGER REFERENCES sessions(session_id),
    status      VARCHAR(20),
    amount      NUMERIC(10, 2),
    ordered_at  TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ad_spend (
    date        DATE,
    channel     VARCHAR(50),
    spend       NUMERIC(10, 2),
    impressions INTEGER,
    clicks      INTEGER,
    PRIMARY KEY (date, channel)
);

CREATE TABLE IF NOT EXISTS email_events (
    event_id    INTEGER PRIMARY KEY,
    customer_id INTEGER REFERENCES customers(customer_id),
    campaign    VARCHAR(100),
    sent_at     TIMESTAMP,
    is_opened   BOOLEAN,
    is_clicked  BOOLEAN
);
