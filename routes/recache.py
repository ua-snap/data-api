from flask import (
    Blueprint,
    current_app as app,
)
import json
import requests
from . import routes

recache_api = Blueprint("recache_api", __name__)

def all_routes():
    all_routes = []
    for rule in app.url_map.iter_rules():
        all_routes.append(rule.rule)
    return all_routes

@routes.route("/cache/")
def recache():
    cf = open('data/jsons/ak_communities.json', 'r')
    communities = json.load(cf)
    hf = open('data/jsons/ak_hucs.json', 'r')
    hucs = json.load(hf)
    paf = open('data/jsons/ak_protected_areas.json', 'r')
    protected_areas = json.load(paf)
    log = open('data/logfile.txt', 'w')
    path = "http://cache.earthmaps.io"
    routes = all_routes()
    for route in routes:
        if (route.find('point') != -1) and (route.find('lat') != -1):
            strip_route = route.replace("<lat>/<lon>", "")
            for community in communities:
                if route.find('var_ep') != -1:
                    for var in ['temperature', 'precipitation', 'taspr']:
                        var_ep_route = strip_route.replace('<var_ep>', var)
                        curr_url = path + var_ep_route + str(community['latitude']) + '/' + str(community['longitude'])
                        print(curr_url)
                        status = requests.get(curr_url)
                        if (status.status_code != 200):
                            log.write(str(status.status_code) + ": " + curr_url + '\n')
                        print(status)
                else:
                    curr_url = path + strip_route + str(community['latitude']) + '/' + str(community['longitude'])
                    print(curr_url)
                    status = requests.get(curr_url)
                    if (status.status_code != 200):
                        log.write(str(status.status_code) + ": " + curr_url + '\n')
                    print(status)
        if (route.find('huc8_id') != -1):
            strip_route = route.replace("<huc8_id>", "")
            for huc in hucs:
                if route.find('var_ep') != -1:
                    for var in ['temperature', 'precipitation', 'taspr']:
                        var_ep_route = strip_route.replace('<var_ep>', var)
                        curr_url = path + var_ep_route + str(huc['id'])
                        print(curr_url)
                        status = requests.get(curr_url)
                        if (status.status_code != 200):
                            log.write(str(status.status_code) + ": " + curr_url + '\n')
                        print(status)
                else:
                    curr_url = path + strip_route + str(huc['id'])
                    print(curr_url)
                    status = requests.get(curr_url)
                    if (status.status_code != 200):
                         log.write(str(status.status_code) + ": " + curr_url + '\n')
                    print(status)
        if (route.find('huc_id') != -1):
            strip_route = route.replace("<huc_id>", "")
            for huc in hucs:
                if route.find('var_ep') != -1:
                    for var in ['temperature', 'precipitation', 'taspr']:
                        var_ep_route = strip_route.replace('<var_ep>', var)
                        curr_url = path + var_ep_route + str(huc['id'])
                        print(curr_url)
                        status = requests.get(curr_url)
                        if (status.status_code != 200):
                            log.write(str(status.status_code) + ": " + curr_url + '\n')
                        print(status)
                else:
                    curr_url = path + strip_route + str(huc['id'])
                    print(curr_url)
                    status = requests.get(curr_url)
                    if (status.status_code != 200):
                        log.write(str(status.status_code) + ": " + curr_url + '\n')
                    print(status)
        if (route.find('akpa_id') != -1):
            strip_route = route.replace("<akpa_id>", "")
            for pa in protected_areas:
                if route.find('var_ep') != -1:
                    for var in ['temperature', 'precipitation', 'taspr']:
                        var_ep_route = strip_route.replace('<var_ep>', var)
                        curr_url = path + var_ep_route + str(pa['id'])
                        print(curr_url)
                        status = requests.get(curr_url)
                        if (status.status_code != 200):
                            log.write(str(status.status_code) + ": " + curr_url + '\n')
                        print(status)
                else:
                    curr_url = path + strip_route + str(pa['id'])
                    print(curr_url)
                    status = requests.get(curr_url)
                    if (status.status_code != 200):
                             log.write(str(status.status_code) + ": " + curr_url + '\n')
                    print(status)
    cf.close()
    hf.close()
    paf.close()
    log.close()
    return json.dumps(routes)