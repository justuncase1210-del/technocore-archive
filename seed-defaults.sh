#!/usr/bin/with-contenv bash
# Seeds /config on first boot of a fresh Akash volume. Gated on the exact
# markers that were missing when this was diagnosed manually (uv itself,
# and archive_api.py) -- so a volume that already has real data is never
# touched, and this is safe to leave running on every restart, not just the
# first.

if [ ! -x /config/.local/bin/uv ]; then
    echo "[seed-defaults] installing uv into /config/.local/bin"
    mkdir -p /config/.local/bin
    cp /opt/uv-seed/uv /config/.local/bin/uv
    chown abc:abc /config/.local/bin/uv
fi

if [ ! -f /config/workspace/archive_api.py ]; then
    echo "[seed-defaults] seeding /config/workspace from baked-in app code"
    mkdir -p /config/workspace
    cp -rn /opt/workspace-seed/. /config/workspace/
    chown -R abc:abc /config/workspace
fi
