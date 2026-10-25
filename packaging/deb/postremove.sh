#!/bin/sh
# The data in /var/lib/iotserver and the iotserver user are kept.
set -e
if [ -d /run/systemd/system ]; then
    systemctl daemon-reload || true
fi
