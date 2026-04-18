#!/bin/bash

# $HOME автоматично містить /Users/dmytriiomelchenko на твоєму Mac
# і /Users/інше_ім'я на комп'ютері колеги
PROJECT_DIR="$HOME/marketing_analytics_pet_project_ae"
DBT_DIR="$PROJECT_DIR/marketing_analytics"
PYTHON="$PROJECT_DIR/venv/bin/python"
NOTIFY="$PROJECT_DIR/scripts/notify_slack.py"

set -e

echo "🚀 Starting pipeline: $(date)"

source "$PROJECT_DIR/venv/bin/activate"

cd "$DBT_DIR"

echo "▶️  Running dbt models..."
if ! dbt run; then
    $PYTHON $NOTIFY failure "dbt run failed. Check logs for details."
    exit 1
fi

echo "🧪 Running dbt tests..."
if ! dbt test; then
    $PYTHON $NOTIFY failure "dbt test failed. Check logs for details."
    exit 1
fi

$PYTHON $NOTIFY success
echo "✅ Pipeline completed successfully: $(date)"
