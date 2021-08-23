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

Example Fire Query:
 -  http://localhost:5000/%F0%9F%94%A5/65.0628/-146.1627
