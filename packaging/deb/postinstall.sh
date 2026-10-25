#!/bin/sh
# Create the service user and its state directory; enable but do not start
# the service, so it never runs with the shipped default password.
set -e
if ! getent passwd iotserver >/dev/null; then
    if command -v adduser >/dev/null; then
        adduser --system --group --home /var/lib/iotserver --no-create-home iotserver
    else
        useradd --system --user-group --home-dir /var/lib/iotserver --shell /usr/sbin/nologin iotserver
    fi
fi
install -d -o iotserver -g iotserver -m 0750 /var/lib/iotserver /var/lib/iotserver/.iotserver
if [ -d /run/systemd/system ]; then
    systemctl daemon-reload
    systemctl enable iotserver.service >/dev/null
fi
cat <<'MSG'
go-iotedge installed. Before starting it:
  1. Write /var/lib/iotserver/.iotserver/iot.json (Password, SessionSecret,
     DBConfig, Security; see the README), owned by iotserver.
  2. sudo systemctl start iotserver
MSG
