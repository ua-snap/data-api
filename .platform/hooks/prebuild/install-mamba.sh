#!/bin/bash
set -e

# Skip if micromamba already installed
if [ -f "/opt/micromamba/bin/micromamba" ]; then
  echo "Micromamba already installed at /opt/micromamba, skipping download and install."
else
  echo "Installing Micromamba..."
  cd /tmp

  MICROMAMBA_VERSION="1.5.8"
  MICROMAMBA_URL="https://micro.mamba.pm/api/micromamba/linux-aarch64/${MICROMAMBA_VERSION}"
  curl -Ls $MICROMAMBA_URL | tar -xvj bin/micromamba
  mkdir -p /opt/micromamba/bin
  mv bin/micromamba /opt/micromamba/bin/
  rmdir bin
  echo "Installed Micromamba version: $MICROMAMBA_VERSION"
fi

# Ensure micromamba is on PATH and initialized
echo 'export PATH="/opt/micromamba/bin:$PATH"' >> /etc/profile.d/conda.sh
echo 'export MAMBA_ROOT_PREFIX="/opt/micromamba"' >> /etc/profile.d/conda.sh
export PATH="/opt/micromamba/bin:$PATH"
export MAMBA_ROOT_PREFIX="/opt/micromamba"

# Check if environment exists
if micromamba env list | grep -q 'api-env'; then
  echo "Micromamba environment 'api-env' already exists, skipping creation."
else
  echo "Creating micromamba environment 'api-env' from environment.yml..."
  micromamba env create -f /var/app/staging/environment.yml
fi

# Always ensure EB will activate the environment on app startup
echo 'eval "$(micromamba shell hook --shell bash)"' >> /etc/profile.d/conda.sh
echo 'micromamba activate api-env' >> /etc/profile.d/conda.sh