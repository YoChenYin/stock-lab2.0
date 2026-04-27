FROM python:3.11-slim
LABEL "language"="python"
LABEL "framework"="fastapi"

ENV PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends cron \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN chmod +x start.sh \
    && cp crontab /etc/cron.d/fetch_daily \
    && chmod 0644 /etc/cron.d/fetch_daily

EXPOSE 8080

CMD ["sh", "start.sh"]
