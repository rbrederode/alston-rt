#!/usr/bin/env bash
set -e

if ! command -v rtl_test >/dev/null; then
    echo "Installing rtl-sdr tools..."
    sudo apt install -y rtl-sdr
fi

echo "=== Update blacklist.conf ==="

BLACKLIST_FILE="/etc/modprobe.d/blacklist.conf"

if ! grep -q "blacklist dvb_usb_rtl28xxu" "$BLACKLIST_FILE"; then
    echo "blacklist dvb_usb_rtl28xxu" | sudo tee -a "$BLACKLIST_FILE"
    echo "Added 'blacklist dvb_usb_rtl28xxu' to $BLACKLIST_FILE"
else
    echo "'blacklist dvb_usb_rtl28xxu' already present in $BLACKLIST_FILE"
fi
if ! grep -q "blacklist rtl2832" "$BLACKLIST_FILE"; then
    echo "blacklist rtl2832" | sudo tee -a "$BLACKLIST_FILE"
    echo "Added 'blacklist rtl2832' to $BLACKLIST_FILE"
else
    echo "'blacklist rtl2832' already present in $BLACKLIST_FILE"
fi
if ! grep -q "blacklist rtl2830" "$BLACKLIST_FILE"; then
    echo "blacklist rtl2830" | sudo tee -a "$BLACKLIST_FILE"
    echo "Added 'blacklist rtl2830' to $BLACKLIST_FILE"
else
    echo "'blacklist rtl2830' already present in $BLACKLIST_FILE"
fi