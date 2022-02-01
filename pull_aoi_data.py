import geopandas as gpd
import json
import os
import pandas as pd
import requests

# Point Locations
path = 'data/csvs/'
if not os.path.exists(path):
    os.makedirs(path)

jsonpath = 'data/jsons/'
if not os.path.exists(jsonpath):
    os.makedirs(jsonpath)

url = 'https://github.com/ua-snap/geospatial-vector-veracity/raw/main/vector_data/point/alaska_point_locations.csv'
r = requests.get(url, allow_redirects=True)
open(f'{path}ak_locations.csv', 'wb').write(r.content)

df = pd.read_csv(f'{path}ak_locations.csv')
df.to_json(f'{jsonpath}ak_locations.json', orient='records')

# HUCs
path = 'data/shapefiles/'
if not os.path.exists(path):
    os.makedirs(path)

for filetype in ['dbf', 'prj', 'sbn', 'sbx', 'shp', 'shx']:
    url = f'https://github.com/ua-snap/geospatial-vector-veracity/blob/main/vector_data/polygon' \
          f'/boundaries/alaska_hucs/hydrologic_units_wbdhu8_a_ak.{filetype}?raw=true '
    r = requests.get(url, allow_redirects=True)
    open(f'{path}hydrologic_units_wbdhu8_a_ak.{filetype}', 'wb').write(r.content)

df = gpd.read_file(f'{path}hydrologic_units_wbdhu8_a_ak.shp')
x = df.copy()
for remove_field in ['geometry','tnmid','metasource','sourcedata','sourceorig','sourcefeat','loaddate','areasqkm','areaacres','referenceg']:
    del x[remove_field]
z = pd.DataFrame(x)
hucs_json = json.loads(z.T.to_json(orient='columns'))
output = []
for key in hucs_json:
    hucs_json[key]['id'] = hucs_json[key]['huc8']
    hucs_json[key]['type'] = 'huc'
    del hucs_json[key]['huc8']
    output.append(hucs_json[key])

with open(f'{jsonpath}ak_hucs.json', 'w') as outfile:
    json.dump(output, outfile)

# Alaska Protected Areas
path = 'data/shapefiles/'
if not os.path.exists(path):
    os.makedirs(path)

for filetype in ['cpg', 'dbf', 'prj', 'shp', 'shx']:
    url = f'https://github.com/ua-snap/geospatial-vector-veracity/blob/main/vector_data/polygon/boundaries' \
          f'/protected_areas/ak_protected_areas/ak_protected_areas.{filetype}?raw=true '
    r = requests.get(url, allow_redirects=True)
    open(f'{path}ak_protected_areas.{filetype}', 'wb').write(r.content)

df = gpd.read_file(f'{path}ak_protected_areas.shp')
x = df.copy()
for remove_field in ['geometry', 'country', 'region']:
    del x[remove_field]
z = pd.DataFrame(x)
pa_json = json.loads(z.T.to_json(orient='columns'))
output = []
for key in pa_json:
    pa_json[key]['type'] = 'protected_area'
    output.append(pa_json[key])

with open(f'{jsonpath}ak_protected_areas.json', 'w') as outfile:
    json.dump(output, outfile)
