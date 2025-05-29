#!/bin/bash
set -e

# Install Miniconda
cd /tmp
CONDA_INSTALLER="Miniconda3-latest-Linux-aarch64.sh"
curl -LO "https://repo.anaconda.com/miniconda/${CONDA_INSTALLER}"
bash $CONDA_INSTALLER -b -p /opt/conda

# Make conda available globally
echo 'export PATH="/opt/conda/bin:$PATH"' >> /etc/profile.d/conda.sh
export PATH="/opt/conda/bin:$PATH"
. /opt/conda/etc/profile.d/conda.sh

# Create and activate conda environment
conda create -y -n api-env -c conda-forge python=3.11 \
  flask flask-cors aiohttp requests marshmallow \
  numpy xarray h5py h5netcdf rioxarray rasterio \
  pyproj shapely geopandas rtree fiona

# Auto-activate it for EB apps
echo 'source activate api-env' >> /etc/profile.d/conda.sh
