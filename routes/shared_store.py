import threading

uploaded_polygons = {}
store_lock = threading.Lock()
POLYGON_EXPIRY_SECONDS = 3600  # 1 hour
