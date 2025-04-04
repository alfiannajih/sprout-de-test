FROM python:3.12.7

WORKDIR /app

# Install cron
RUN apt-get update && apt-get install -y cron

# Copy crontab
COPY crontab /etc/cron.d/etl-cron
RUN chmod 0644 /etc/cron.d/etl-cron

# Copy requirements and install
COPY ./requirements.txt requirements.txt
RUN pip install -r requirements.txt

# Copy your script
COPY ./etl/ .

RUN chmod +x /app/run_etl.sh

# Apply cron job
RUN crontab /etc/cron.d/etl-cron

# Create the log file to be able to run tail
RUN touch /var/log/cron.log

# Start cron and keep container alive
CMD cron && tail -f /var/log/cron.log