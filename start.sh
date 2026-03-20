#!/bin/bash
set -e

echo "=== SEC 13F Insights Dashboard ==="

if [ ! -f edgar_13f.db ]; then
    echo "No database found. Seeding from SEC EDGAR (this takes 1-2 minutes)..."
    python pipeline.py --top-managers 5 --start 2023-01-01
    echo "Database seeded successfully."
else
    echo "Database found, skipping seed."
fi

echo "Starting Streamlit on port ${PORT:-8501}..."
exec streamlit run Phase1_Insights/app.py \
    --server.port "${PORT:-8501}" \
    --server.address 0.0.0.0
