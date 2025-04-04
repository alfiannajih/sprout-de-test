#!/bin/bash

cd /app

# Get today's date
TODAY=$(date +%F)
DATE_LOG="$(date +"%Y-%m-%d %H:%M:%S")"

# Retry up to 3 times with 10-second delay
MAX_RETRIES=3
RETRY_DELAY=10
COUNT=0

while [ $COUNT -lt $MAX_RETRIES ]; do
    echo "[$DATE_LOG] Attempt $(($COUNT + 1)) running main.py with --date $TODAY" >> /var/log/cron.log
    /usr/local/bin/python main.py --date "$TODAY" >> /var/log/cron.log 2>&1

    EXIT_CODE=$?
    if [ $EXIT_CODE -eq 0 ]; then
        echo "[$DATE_LOG] Success!" >> /var/log/cron.log
        break
    else
        echo "[$DATE_LOG] Failed with exit code $EXIT_CODE. Retrying in $RETRY_DELAY seconds..." >> /var/log/cron.log
        sleep $RETRY_DELAY
        COUNT=$(($COUNT + 1))
    fi
done

if [ $COUNT -eq $MAX_RETRIES ]; then
    echo "[$DATE_LOG] All retries failed." >> /var/log/cron.log
fi