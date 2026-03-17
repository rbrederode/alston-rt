#!/usr/bin/env bash
# bootstrap-pi.sh
# Purpose: Fix apt issues, install build dependencies, and prepare pyenv + pyenv-virtualenv
# Usage: sudo bash bootstrap-pi.sh

set -euo pipefail

echo "=== 1. Fix broken packages and held libraries ==="
sudo apt --fix-broken install -y || true
sudo dpkg --configure -a || true

# Unhold libssl if held
for pkg in libssl-dev libssl3t64; do
    if dpkg --get-selections | grep -q "^$pkg[[:space:]]*hold"; then
        echo "Unholding $pkg"
        sudo apt-mark unhold "$pkg"
    fi
done

echo "=== 2. Clean apt lists ==="
sudo apt clean
sudo rm -rf /var/lib/apt/lists/*
sudo apt update

echo "=== 3. Force install matching libssl versions ==="
sudo apt install -y \
    libssl-dev=3.5.5-1~deb13u1 \
    libssl3t64=3.5.5-1~deb13u1 \
    --allow-downgrades || true

echo "=== 4. Install Python build dependencies ==="
sudo apt install -y \
    git curl build-essential zlib1g-dev \
    libbz2-dev libreadline-dev libsqlite3-dev wget llvm \
    libncurses-dev xz-utils tk-dev \
    libffi-dev liblzma-dev libgdbm-dev libnss3-dev

echo "=== 5. Install pyenv and pyenv-virtualenv ==="
if [ ! -d "$HOME/.pyenv" ]; then
    git clone https://github.com/pyenv/pyenv.git ~/.pyenv
fi

if [ ! -d "$HOME/.pyenv/plugins/pyenv-virtualenv" ]; then
    git clone https://github.com/pyenv/pyenv-virtualenv.git ~/.pyenv/plugins/pyenv-virtualenv
fi

# Setup shell environment for pyenv
export PYENV_ROOT="$HOME/.pyenv"
export PATH="$PYENV_ROOT/bin:$PATH"

if ! command -v pyenv >/dev/null; then
    echo "Initializing pyenv..."
    eval "$(pyenv init --path)"
    eval "$(pyenv init -)"
    eval "$(pyenv virtualenv-init -)"
fi

echo "=== 6. Create default virtualenv 'venv' if it doesn't exist ==="
PYTHON_VERSION="3.14.3"   
if ! pyenv versions --bare | grep -q "$PYTHON_VERSION"; then
    echo "Installing Python $PYTHON_VERSION..."
    pyenv install "$PYTHON_VERSION"
fi

if ! pyenv virtualenvs --bare | grep -q "venv"; then
    echo "Creating pyenv virtualenv 'venv'..."
    pyenv virtualenv "$PYTHON_VERSION" venv
fi

echo "=== 7. Activate virtualenv ==="
pyenv activate venv

echo "Bootstrap complete! You can now use 'pyenv activate venv'"