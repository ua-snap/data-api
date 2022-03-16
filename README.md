# Data API for SNAP@IARC

## Installing

`pipenv install`

## Running application

`export FLASK_APP=application.py`

`export FLASK_ENV=development`

Set the GeoServer/Rasdaman endpoints if needed:

`export API_GS_BASE_URL=http://...`

`export API_RAS_BASE_URL=http://...`

`pipenv run flask run`


Example Permafrost Query:
 -  http://localhost:5000/permafrost/point/65.0628/-146.1627
 -  http://localhost:5000/permafrost/point/52.906/-148.345

Example Fire Query:
 -  http://localhost:5000/fire/point/65.0628/-146.1627
 -  http://localhost:5000/fire/point/52.906/-148.345

Example Glacier Query:
 - http://localhost:5000/glacier/point/60.606/-143.345
 - http://localhost:5000/glacier/point/52.906/-148.345

Example Geology Query
 - http://localhost:5000/geology/point/69.606/-145.345
 - http://localhost:5000/geology/point/56.606/-143.345

Example Physiography Query
 - http://localhost:5000/physiography/point/64.606/-147.345

## Updating Production API + Varnish Cache

Due to the configuration of our services, we need to both update
our API instance + restart our Varnish cache when necessary.

Instructions for this can be found here:

 - [SNAP Data API Instructions](https://docs.google.com/document/d/1Z31-mkDE0skITOuOOMBQwuO2I8jUDuApm7VX-A9v1LA/edit?usp=sharing)