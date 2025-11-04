from flask import Blueprint, request, jsonify, render_template, current_app
import geopandas as gpd
import io, zipfile, secrets, time, tempfile, os
from datetime import datetime, timezone

from . import routes

upload_polygon = Blueprint("upload_polygon", __name__)


@routes.route("/upload_polygon", methods=["POST"])
def upload_polygon():
    """
    Accepts an uploaded shapefile (as a ZIP containing .shp, .shx, .dbf, .prj)
    and stores the geometry in memory with a unique ID. The ID can then be used to
    reference the polygon in subsequent requests from other endpoints. Optionally,
    a user-defined name for the polygon can be provided via form data - this would
    be used for downstream display purposes, for naming output files, etc.

    Upload via curl example:
        curl -F "file=@my_shapefile.zip" \
             -F "name=my custom polygon" \
             http://localhost:5000/upload_polygon 

    #TODO: implement form in HTML template for browser-based upload
    #TODO: implement additional validation (file size limit, filename checks, upload limit per IP, etc.)
    #TODO: implement cleanup of old in-memory polygons after expiry time (e.g., 1 hour)
    #TODO: encourage users to create polygons via https://geojson.io/#map=3.92/63/-154.56 , which
    # automatically zips the shapefile components for download.

    Example response JSON:

        {
            "expires_at": "2025-11-04T18:48:02.522820+00:00",
            "name": "my custom polygon",
            "polygon_id": "f-EMX4YL",
            "uploaded_at": "2025-11-04T17:48:02.522820+00:00"
        }

    Example of current_app.uploaded_polygons content after upload:

        {
            'f-EMX4YL': {
                'geometry': <POLYGON ((-147.626 64.942, -147.688 64.966, -147.794 64.97, -147.901 64.948...>,
                'expires_at': "2025-11-04T18:48:02.522820+00:00",
                'uploaded_at': "2025-11-04T17:48:02.522820+00:00",
                'name': 'my custom polygon'
            }
        }

    Returns:
        JSON with polygon_id, polygon name, and expiry time, or error message.
    """

    # check for file in request
    if "file" not in request.files:
        return (
            jsonify({"error": "No file part in request"}),
            400,
        )
    file = request.files["file"]
    if file.filename == "":
        return (
            jsonify({"error": "No selected file"}),
            400,
        )

    # optional user-defined polygon name
    user_name = request.form.get("name", "").strip()
    if not user_name:
        user_name = "unnamed_polygon"

    # read the uploaded zip into memory
    file_bytes = file.read()

    # validate that it's a zip containing a .shp file
    try:
        with zipfile.ZipFile(io.BytesIO(file_bytes)) as zf:
            shapefile_names = [n for n in zf.namelist() if n.lower().endswith(".shp")]
            if not shapefile_names:
                return (
                    jsonify({"error": "No .shp file found in ZIP"}),
                    400,
                )
    except zipfile.BadZipFile:
        return (
            jsonify({"error": "Uploaded file is not a valid ZIP archive"}),
            400,
        )

    # write zip file contents to a temporary file so GeoPandas/Fiona can read it

    # NOTE: this actually writes to disk because GeoPandas (via Fiona) cannot read
    # directly from an in-memory object (BytesIO) when dealing with shapefiles inside ZIP archives.
    # Shapefiles consist of multiple files (.shp, .shx, .dbf, .prj) that need to be accessed together.
    # Fiona delegates to GDAL, which expects either a folder path, or a “virtual file system” path
    # like zip://path_to_zip!inner_path.
    # we write the uploaded ZIP to disk temporarily and give GDAL that real path,
    # then delete the temp file after reading.

    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
        tmp.write(file_bytes)
        tmp_path = tmp.name

    try:
        with zipfile.ZipFile(tmp_path) as zf:
            shapefile_names = [n for n in zf.namelist() if n.lower().endswith(".shp")]
            if not shapefile_names:
                return (
                    jsonify({"error": "No .shp file found in ZIP"}),
                    400,
                )
            shapefile_path = shapefile_names[0]

        # note the exclamation mark syntax:
        gdf = gpd.read_file(f"zip://{tmp_path}!{shapefile_path}")

    except Exception as e:
        return (
            jsonify({"error": f"Invalid shapefile ZIP: {str(e)}"}),
            400,
        )
    finally:
        # clean up temporary file
        if os.path.exists(tmp_path):
            os.remove(tmp_path)

    if gdf.empty:
        return (
            jsonify({"error": "Uploaded shapefile contains no features"}),
            400,
        )

    # combine all polygons into one geometry
    # if the user uploads a shapefile with multiple features, they are treated as one feature
    # we could decide to loop through each feature and process them separately,
    # but for now we just union them to force a single geometry
    geom = gdf.unary_union

    # generate a unique polygon ID
    poly_id = secrets.token_urlsafe(6)

    # store geometry, upload time, and user-defined name in the app's in-memory store

    upload_time = datetime.fromtimestamp(time.time(), tz=timezone.utc).isoformat()
    # 3600 second = 1 hour expiry
    expiry_time = datetime.fromtimestamp(
        time.time() + 3600, tz=timezone.utc
    ).isoformat()

    with current_app.store_lock:
        current_app.uploaded_polygons[poly_id] = {
            "geometry": geom,
            "uploaded_at": upload_time,
            "expires_at": expiry_time,
            "name": user_name,
        }

    return (
        jsonify(
            {
                "polygon_id": poly_id,
                "name": user_name,
                "expires_at": expiry_time,
                "uploaded_at": upload_time,
            }
        ),
        200,
    )
