#!/bin/sh
set -e
if [ -d /run/systemd/system ] && [ "$1" = remove ]; then
    systemctl stop iotserver.service || true
    systemctl disable iotserver.service >/dev/null || true
fi
