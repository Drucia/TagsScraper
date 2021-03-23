import sys
import pandas as pd
from tqdm import tqdm
import requests
import json
import os
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

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
    EDUCATION = ["school", "university", "language_school"]
    HEALTH = ["hospital"]
    TRANSPORT = ["bus_station"]
    SHOPPING_MALL = ["mall", "cinema"]

def scrap_tag_from(json_resp):
    nodes = json_resp['elements']
    all_tags = list(map(scrap_tag, nodes))

    tags_counter = {
        "education" : count_tags(tags.EDUCATION, all_tags),
        "health": count_tags(tags.HEALTH, all_tags),
        "transport": count_tags(tags.TRANSPORT, all_tags),
        "shopping": count_tags(tags.SHOPPING_MALL, all_tags)
    }

    best = max(tags_counter, key=tags_counter.get)
    return "other" if tags_counter[best] == 0 else best

def count_tags(tag_names, tags_to_check):
    return sum([tags_to_check.count(tag) for tag in tag_names])

def scrap_tag(node):
    tags_of_node = node['tags']
    return tags_of_node['amenity'] if 'amenity' in tags_of_node else tags_of_node['shop']

def requests_retry_session(
    retries=5,
    backoff_factor=0.3,
    status_forcelist=(500, 502, 504),
    session=None,
):
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

def main():
    if (len(sys.argv) < 3):
        print(f'{bcolors.FAIL}❌ Not enought params in input')
        print(bcolors.DEFAULT)
        return;

    inputFile = sys.argv[1]
    outFile = sys.argv[2]
    around = sys.argv[3] if (len(sys.argv) > 3) else 300
    tmpFile = sys.argv[4] if (len(sys.argv) > 4) else "tmp.csv"

    coordinates_df = pd.read_csv(inputFile)
    tags_results = []

    if not os.path.exists(tmpFile):
        tmp = pd.DataFrame([], columns=["number", "tag", "request"])
        tmp.to_csv(tmpFile, index=False)

    data_shape = coordinates_df.shape[0]
    for index, row in tqdm(coordinates_df.iterrows(), total=data_shape):
        print(f'\n🔽{bcolors.OKBLUE} Tag number: {row["number"]}')

        request_url = f"""https://overpass-api.de/api/interpreter?data=[out:json]; 
                        (
                            node['amenity'~'school|university|hospital|bus_station|cinema'](around:{around}, {row['lat']}, {row['lng']});
                            node['shop'~'mall'](around:{around}, {row['lat']}, {row['lng']});
                        );
                        out;"""
        response = requests_retry_session().get(request_url)

        if (response.status_code != 200):
            print(f'{bcolors.FAIL}❌ Request failed for tag number: {row["number"]}')

            to_add = [row['number'], "unknown", request_url]
            tmp = pd.DataFrame([to_add], columns=["number", "tag", "request"])
            tmp.to_csv(tmpFile, mode='a', header=False, index=False)
        else:
            json_resp = json.loads(response.text)
            tag_for_node = scrap_tag_from(json_resp)

            to_add = [row['number'], tag_for_node]
            tags_results.append(to_add)

            tmp_to_add = [row['number'], tag_for_node, ""]
            tmp = pd.DataFrame([tmp_to_add], columns=["number", "tag", "request"])
            tmp.to_csv(tmpFile, mode='a', header=False, index=False)
        print(bcolors.OKGREEN)

    tags_df = pd.DataFrame(tags_results, columns=["number", "tag"])
    tags_df.to_csv(outFile, index=False)
    print(f'\n🔽{bcolors.OKGREEN} All tags: {len(tags_results)} was save successfully to: {outFile}')
    print(bcolors.DEFAULT)

def scrap_unknown():
    if (len(sys.argv) < 3):
        print(f'{bcolors.FAIL}❌ Not enought params in input')
        print(bcolors.DEFAULT)
        return;

    tmpFile = sys.argv[1]
    outFile = sys.argv[2]

    tmpFile_df = pd.read_csv(tmpFile)
    tags_results = []

    if not os.path.exists(outFile):
        tmp = pd.DataFrame([], columns=["number", "tag"])
        tmp.to_csv(outFile, index=False)

    data_shape = tmpFile_df.shape[0]
    for index, row in tqdm(tmpFile_df.iterrows(), total=data_shape):
        print(f'\n🔽{bcolors.OKBLUE} Tag number: {row["number"]}')

        if row['tag'] in "unknown":
            print(f'\n⏭️{bcolors.WARNING}  Tag number: {row["number"]} skipped')
            continue

        response = requests_retry_session().get(row['request'])

        if (response.status_code == 200):
            json_resp = json.loads(response.text)
            tag_for_node = scrap_tag_from(json_resp)
            tmp_to_add = [row['number'], tag_for_node]
            tmp = pd.DataFrame([tmp_to_add], columns=["number", "tag"])
            tmp.to_csv(outFile, mode='a', header=False, index=False)
        print(bcolors.OKGREEN)

    print(f'\n🔽{bcolors.OKGREEN} All tags: {len(tags_results)} was save successfully to: {outFile}')
    print(bcolors.DEFAULT)

main()
# scrap_unknown()
