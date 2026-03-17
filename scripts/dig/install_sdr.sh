#!/usr/bin/env bash
set -e

if ! command -v rtl_test >/dev/null; then
    echo "Installing rtl-sdr tools..."
    sudo apt install -y rtl-sdr
fi