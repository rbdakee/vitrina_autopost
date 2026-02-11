#!/usr/bin/env bash
set -euo pipefail

# Скрипт запуска python
cat >/usr/local/bin/run_job.sh <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
source /etc/profile.d/container_env.sh || true
cd /app
python auto_post.py
EOF
chmod +x /usr/local/bin/run_job.sh

# Cron не всегда видит env контейнера — сохраняем env
printenv | sed 's/^\(.*\)$/export \1/g' > /etc/profile.d/container_env.sh
chmod +x /etc/profile.d/container_env.sh

# Устанавливаем crontab
crontab /etc/cron.d/app-crontab

# ✅ СРАЗУ запускаем один прогон
echo "[entrypoint] Initial run..."
/usr/local/bin/run_job.sh >> /var/log/cron.log 2>&1 || true
echo "[entrypoint] Initial run done."

# Запускаем cron
service cron start

# Хвостим лог
touch /var/log/cron.log
tail -F /var/log/cron.log
