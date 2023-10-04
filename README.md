# Data API for SNAP@IARC

## Installing

`pipenv install`

## Running application

Set `flask` application environment variables:

`export FLASK_APP=application.py`

`export FLASK_DEBUG=True`

Set the GeoServer/Rasdaman URL environment variables. (If not set using environment variables, these will default to the URLs found in `config.py`.):

`export API_GS_BASE_URL=http://...`

`export API_RAS_BASE_URL=https://apollo.snap.uaf.edu/rasdaman/`

Run the application:

`pipenv run flask run`

## Query API endpoints

Example Permafrost Query:

- http://localhost:5000/permafrost/point/gipl/65.0628/-146.1627
- http://localhost:5000/permafrost/point/gipl/52.906/-148.345

Example Fire Query:

- http://localhost:5000/fire/point/65.0628/-146.1627
- http://localhost:5000/fire/point/52.906/-148.345

Example Geology Query

- http://localhost:5000/geology/point/69.606/-145.345
- http://localhost:5000/geology/point/56.606/-143.345

Example Physiography Query

- http://localhost:5000/physiography/point/64.606/-147.345

## Updating Production API + Varnish Cache

Due to the configuration of our services, we need to both update
our API instance + restart our Varnish cache when necessary.

Prior to deploying, update the `requirements.txt`:

```bash
pipenv run pip freeze > requirements.txt 
git commit -am'update requirements.txt'
```

Instructions for this can be found here:

- [SNAP Data API Instructions](https://docs.google.com/document/d/1Z31-mkDE0skITOuOOMBQwuO2I8jUDuApm7VX-A9v1LA/edit?usp=sharing)
