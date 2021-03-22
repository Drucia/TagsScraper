import sys
import pandas as pd
from tqdm import tqdm
import requests
import json

timeout = 10  # in seconds

class bcolors:
    DEFAULT = "\033[39m"
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

class tags:
    EDUCATION = ["school", "university"]
    HEALTH = ["hospital", "pharmacy"]
    TRANSPORT = ["bus_station"]
    SHOPPING_MALL = ["mall", "cinema"]

def scrap_tags_from(json_resp):
    nodes = json_resp['elements']
    for node in nodes:


def main():
    if (len(sys.argv) < 3):
        print(f'{bcolors.FAIL}❌ Not enought params in input')
        print(bcolors.DEFAULT)
        return;

    inputFile = sys.argv[1]
    outFile = sys.argv[2]
    around = sys.argv[3] if (len(sys.argv) == 4) else 500

    coordinatesDf = pd.read_csv(inputFile)
    for index, row in tqdm(coordinatesDf.iterrows()):
        print(
            f'\n🔽{bcolors.OKBLUE} Tags number: {index + 1}...')

        request_url = f"""https://overpass-api.de/api/interpreter?data=[out:json]; 
                        (
                            node['amenity'~'school|university|hospital|pharmacy|bus_station'|'cinema'](around:{around}, {row['lat']}, {row['lng']});
                            node['shop'~'mall'](around:{around}, {row['lat']}, {row['lng']});
                        );
                        out;"""
        response = requests.get(request_url, timeout=timeout)
        json_resp = json.loads(response.text)
        scrap_tags_from(json_resp)
        return

main()