#!/bin/bash
set -e

# Skip if conda already installed
if [ -d "/opt/conda" ]; then
  echo "Miniconda already installed at /opt/conda, skipping download and install."
else
  echo "Installing Miniconda..."
  cd /tmp
  CONDA_INSTALLER="Miniconda3-latest-Linux-aarch64.sh"
  curl -LO "https://repo.anaconda.com/miniconda/${CONDA_INSTALLER}"
  bash $CONDA_INSTALLER -b -p /opt/conda
fi

# Ensure conda is on PATH and initialized
echo 'export PATH="/opt/conda/bin:$PATH"' >> /etc/profile.d/conda.sh
export PATH="/opt/conda/bin:$PATH"
. /opt/conda/etc/profile.d/conda.sh

# Check if environment exists
if conda info --envs | grep -q 'api-env'; then
  echo "Conda environment 'api-env' already exists, skipping creation."
else
  echo "Creating conda environment 'api-env'..."
  conda create -y -n api-env -c conda-forge python=3.11 \
    flask flask-cors gunicorn aiohttp requests marshmallow \
    numpy xarray h5py h5netcdf rioxarray rasterio \
    pyproj shapely geopandas rtree fiona
fi

# Always ensure EB will activate the environment on app startup
echo 'source activate api-env' >> /etc/profile.d/conda.sh
