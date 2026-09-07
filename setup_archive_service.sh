#!/bin/bash
# Re-creates the svc-archive-api s6 service so archive_api.py auto-starts and
# auto-restarts on crash. Safe to re-run any time -- idempotent.
# Must be run as root (the Shell tab, root@codeserver-0 prompt).
set -e
mkdir -p /etc/s6-overlay/s6-rc.d/svc-archive-api/dependencies.d
echo "longrun" > /etc/s6-overlay/s6-rc.d/svc-archive-api/type
echo "3" > /etc/s6-overlay/s6-rc.d/svc-archive-api/notification-fd
touch /etc/s6-overlay/s6-rc.d/svc-archive-api/dependencies.d/init-services
cat > /etc/s6-overlay/s6-rc.d/svc-archive-api/run << 'RUNEOF'
#!/usr/bin/with-contenv bash
export HOME=/config
cd /config/workspace || exit 1
exec s6-notifyoncheck -d -n 300 -w 1000 -c "nc -z 127.0.0.1 8000" s6-setuidgid abc /config/.local/bin/uv run archive_api.py
RUNEOF
chmod +x /etc/s6-overlay/s6-rc.d/svc-archive-api/run
touch /etc/s6-overlay/s6-rc.d/user/contents.d/svc-archive-api
echo "svc-archive-api service (re)installed."
