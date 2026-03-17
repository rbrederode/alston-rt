#!/usr/bin/env bash
set -e

echo "Updating package lists..."
sudo apt update

echo "Upgrading system..."
sudo apt upgrade -y