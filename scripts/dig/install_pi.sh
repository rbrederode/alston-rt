#!/usr/bin/env bash
set -e

echo "==================================="
echo " Raspberry Pi Digitiser Installer. "
echo "==================================="

BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "Running system update..."
bash "$BASE_DIR/scripts/system_update.sh"

echo "Installing packages..."
bash "$BASE_DIR/scripts/install_packages.sh"

echo "Installing SDR drivers..."
bash "$BASE_DIR/scripts/install_sdr.sh"

echo "Setting up Python environment..."
bash "$BASE_DIR/scripts/install_python.sh"

echo "Configuring services..."
#bash "$BASE_DIR/scripts/configure_services.sh"

echo ""
echo "Installation complete."