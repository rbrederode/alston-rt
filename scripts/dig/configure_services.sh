#!/usr/bin/env bash
set -e

SERVICE_FILE="alston-rt.service"

sudo cp "$(dirname "$0")/$SERVICE_FILE" /etc/systemd/system/

sudo systemctl daemon-reload
sudo systemctl enable alston-rt