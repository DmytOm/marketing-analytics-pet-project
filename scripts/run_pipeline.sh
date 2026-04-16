#!/bin/bash

# Exit immediately if any command fails
set -e

echo "🚀 Starting pipeline: $(date)"

# Activate virtual environment
source /Users/dmytriiomelchenko/marketing_analytics_pet_project_ae/venv/bin/activate

# Navigate to dbt project
cd /Users/dmytriiomelchenko/marketing_analytics_pet_project_ae/marketing_analytics

# Run dbt
echo "▶️  Running dbt models..."
dbt run

echo "🧪 Running dbt tests..."
dbt test

echo "✅ Pipeline completed successfully: $(date)"
