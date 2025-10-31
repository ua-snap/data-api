# Data API for SNAP@IARC

## Installing

Install Micromamba via brew:

```
brew install micromamba
```

Create a new `mamba` environment like so:

```
micromamba env create -f environment.yml
```

This creates a Mamba environment called api-env which you can activate:

```
micromamba activate api-env
```

## Running application

Set `flask` application environment variables:

```
export FLASK_APP=application.py
export FLASK_DEBUG=True
```

Review environment variables found in `config.py`, and reset them for development if necessary (e.g., `export API_GS_BASE_URL=https://gs-dev.earthmaps.io/geoserver/`).

Start the application via your `mamba` environment:

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

## Creating and updating Production API + Varnish Cache

To create a new ElasticBeanstalk API instance, do the following:

```
git checkout production
git pull
eb init <-- Choose us-west-2 and data-api-production for application
eb create --it r7g.large --single
```

To update the current API instance, do the following:

```
git checkout production
git pull
eb deploy
```

Explicit instructions for creating and updating the API can be found here:

- [SNAP Data API Instructions](https://docs.google.com/document/d/18-pEC-Rri3EQcNXaHajhqMYmRc_LBX1p3wWKoYnC874/edit?tab=t.jzrka8gsdrfw)
