FROM python:3.11-slim

WORKDIR /app

# cron, bash, ffmpeg для уникализации видео
RUN apt-get update \
 && apt-get install -y --no-install-recommends cron bash ca-certificates ffmpeg \
 && rm -rf /var/lib/apt/lists/*

# Зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Приложение и конфиг cron
COPY auto_post.py unique.py /app/
COPY crontab.txt /etc/cron.d/app-crontab
COPY entrypoint.sh /entrypoint.sh

RUN sed -i 's/\r$//' /entrypoint.sh /etc/cron.d/app-crontab \
 && chmod 0644 /etc/cron.d/app-crontab \
 && chmod +x /entrypoint.sh \
 && touch /var/log/cron.log

CMD ["/entrypoint.sh"]
