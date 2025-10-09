# Data API for SNAP@IARC

## Installing

Running the API requires Python 3.11. 

Use `pipenv` to create a virtual environment from the repo's `Pipfile` using:
```
pipenv install
```

Alternatively, create a new `conda` environment like so:

```
conda create -n api python=3.11
conda activate api
conda install -c conda-forge flask flask-cors gunicorn aiohttp requests marshmallow numpy xarray h5py h5netcdf rioxarray rasterio pyproj shapely geopandas rtree fiona jaro-winkler
```

## Running application

Set `flask` application environment variables:

```
export FLASK_APP=application.py
export FLASK_DEBUG=True
```


Review environment variables found in `config.py`, and reset them for development if necessary (e.g., `export API_GS_BASE_URL=https://gs-dev.earthmaps.io/geoserver/`).


Start the application via `pip`:

```
pipenv run flask run
```

Or alternatively, from your `conda` environment:

```
flask run
```

## Query API endpoints

Example Permafrost Query:

- http://localhost:5000/permafrost/point/gipl/65.0628/-146.1627
- http://localhost:5000/permafrost/point/gipl/62.906/-148.345

Example Fire Query:

- http://localhost:5000/fire/point/65.0628/-146.1627
- http://localhost:5000/fire/point/62.906/-148.345

Example Precipitation Query:

- http://localhost:5000/precipitation/point/65.028/-146.1627
- http://localhost:5000/precipitation/point/62.906/-148.345


## Updating Production API + Varnish Cache

Due to the configuration of our services, we need to both update our API instance + restart our Varnish cache when necessary.

Prior to deploying, update the `requirements.txt`:

```bash
pipenv run pip freeze > requirements.txt
git commit -am'update requirements.txt'
```

Instructions for this can be found here:

- [SNAP Data API Instructions](https://docs.google.com/document/d/1Z31-mkDE0skITOuOOMBQwuO2I8jUDuApm7VX-A9v1LA/edit?usp=sharing)
