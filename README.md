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
 -  http://localhost:5000/permafrost/65.0628/-146.1627
 -  http://localhost:5000/permafrost/52.906/-148.345

Example Fire Query:
 -  http://localhost:5000/%F0%9F%94%A5/65.0628/-146.1627
 -  http://localhost:5000/%F0%9F%94%A5/52.906/-148.345

Example Glacier Query:
 - http://localhost:5000/glacier/60.606/-143.345
 - http://localhost:5000/glacier/52.906/-148.345

Example Geology Query
 - http://localhost:5000/geology/69.606/-145.345
 - http://localhost:5000/geology/56.606/-143.345

Example Physiography Query
 - http://localhost:5000/physiography/64.606/-147.345
