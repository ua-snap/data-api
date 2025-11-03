from flask import Blueprint, request, jsonify
import geopandas as gpd
import io, zipfile, uuid, time

from .shared_store import uploaded_polygons, store_lock, POLYGON_EXPIRY_SECONDS

byop = Blueprint("byop", __name__)


@byop.route("/upload_polygon", methods=["POST"])
def upload_polygon():
    """
    Accepts an uploaded shapefile (as a ZIP containing .shp, .shx, .dbf, .prj)
    and stores the geometry in memory with a UUID key. UUID can then be used to
    reference the polygon in subsequent requests from other endpoints.

    #TODO: encourage users to create polygons via https://geojson.io/#map=3.92/63/-154.56

        Upload via curl example:
            curl -F "file=@my_shapefile.zip" http://localhost:5000/upload_polygon

        UUID example response:
            {
                "polygon_id": "byop-123e4567-e89b-12d3-a456-426614174000",
                "expires": 1701301234.56789

    Returns:
        JSON with polygon_id and expiry time, or error message.
    """

    if "file" not in request.files:
        return jsonify({"error": "No file part in request"}), 400

    file = request.files["file"]
    if file.filename == "":
        return jsonify({"error": "No selected file"}), 400

    # Read the uploaded zip into memory
    file_bytes = file.read()
    zip_bytes = io.BytesIO(file_bytes)

    try:
        # Verify it's a valid zip
        with zipfile.ZipFile(zip_bytes) as zf:
            # Find the shapefile (.shp) inside the zip
            shapefile_names = [n for n in zf.namelist() if n.lower().endswith(".shp")]
            if not shapefile_names:
                return jsonify({"error": "No .shp file found in ZIP"}), 400

            # Read shapefile directly from the ZIP into GeoPandas
            with zf.open(shapefile_names[0]) as shp_file:
                # geopandas can’t read directly from ZipFile, so we need to use the virtual path
                # tip: geopandas supports “zip://” syntax
                zip_path = f"/vsizip/{shp_file.name}"
                # However, in-memory zip reading with /vsizip requires fsspec or fiona’s path syntax:
                # Simplest solution: write the zip into a temporary in-memory file object for fiona:
                zf_bytes = io.BytesIO(file_bytes)
                gdf = gpd.read_file(f"zip://{zf_bytes}")

    except Exception as e:
        return jsonify({"error": f"Invalid shapefile ZIP: {str(e)}"}), 400

    if gdf.empty:
        return jsonify({"error": "Uploaded shapefile contains no features"}), 400

    # For simplicity, take the union of all polygons (or just store the list)
    geom = gdf.unary_union  # shapely geometry

    # Generate UUID and store
    poly_id = str("byop-" + uuid.uuid4())
    with store_lock:
        uploaded_polygons[poly_id] = (geom, time.time())

    return (
        jsonify(
            {
                "polygon_id": poly_id,
                # use current time and POLYGON_EXPIRY_SECONDS to get expiry time as a timestamp
                "expires": time.time() + POLYGON_EXPIRY_SECONDS,
            }
        ),
        200,
    )
