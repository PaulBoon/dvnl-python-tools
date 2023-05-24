#!/usr/bin/env python3

# Collect the storage usage (in bytes) for (sub-)dataverses (collections) of a dataverse instance in a csv table.

###
# prerequisites:
#   install the requests module:
#   python3 -m pip install requests
#
import requests

import argparse
import csv
import re


# Note that there is no correct error handling and also no tests

def get_storage_size_msg(api_token, server_url, alias):
    headers = {'X-Dataverse-key': api_token}
    url = server_url + "/api/dataverses/" + alias + "/storagesize"
    dv_resp = requests.get(url, headers=headers)
    # print(dv_resp.json())
    resp_data = dv_resp.json()['data']
    # print(resp_data['message'])
    return resp_data['message']


def extract_size_str(msg):
    # Example message: "Total size of the files stored in this dataverse: 43,638,426,561 bytes"
    # try parsing the size from this string.
    size_found = re.search('dataverse: (.+?) bytes', msg).group(1)
    # remove those ',' delimiters and optionally '.' as well,
    # depending on locale (there is no fractional byte!).
    # Delimiter are probably there to improve readability of those large numbers,
    # but calculating with it is problematic.
    clean_size_str = size_found.translate({ord(i): None for i in ',.'})
    return clean_size_str


# Traverses the tree and collect sizes for ech dataverse using recursion.
# Note that storing the parents size if all children sizes are also stored is redundant.
def get_children_sizes(api_token, server_url, parent_data, max_depth, depth=1):
    parent_alias = parent_data['alias']
    child_result_list = []
    # only direct descendants (children)
    if 'children' in parent_data:
        for i in parent_data['children']:
            child_alias = i['alias']
            print("Retrieving size for dataverse: {} / {} ...".format(parent_alias, child_alias))
            msg = get_storage_size_msg(api_token, server_url, child_alias)
            storagesize = extract_size_str(msg)
            row = {'depth': depth, 'parentalias': parent_alias, 'alias': child_alias, 'name': i['name'],
                   'storagesize': storagesize}
            child_result_list.append(row)
            if depth < max_depth:
                child_result_list.extend(get_children_sizes(api_token, server_url, i, depth + 1))  # recurse
    return child_result_list


# The dataverse tree (hierarchy of sub-verses) is public metrics,
# so only published info can be retrieved.
def get_tree(server_url):
    url = server_url + "/api/info/metrics/tree"
    dv_resp = requests.get(url)
    resp_data = dv_resp.json()['data']
    return resp_data


def collect_storage_usage(api_token, server_url, output_filename, max_depth, include_grand_total):
    print("Start collecting storage usage for: " + args.server_url)

    # construct the result dictionary
    result_list = []

    tree_data = get_tree(server_url)

    alias = tree_data['alias']
    name = tree_data['name']
    print("Extracted the tree for the toplevel dataverse: {} ({})".format(name, alias))
    # could now get total size for this dataverse, or skip and just do the children,
    # adding up could always be done afterwards if needed
    # Note that requesting this make the processing time increase dramatically (almost double)
    if args.include_grand_total:
        print("Retrieving total size for this dataverse instance")
        msg = get_storage_size_msg(api_token, server_url, alias)
        storagesize = extract_size_str(msg)
        row = {'depth': 0, 'parentalias': alias, 'alias': alias, 'name': name,
               'storagesize': storagesize}
        result_list.append(row)

    result_list.extend(get_children_sizes(api_token, server_url, tree_data, max_depth, 1))

    # store it as csv
    csv_filename = output_filename
    # might be nice to add timestamp or maybe a counter to the filename if it already exists
    csv_columns = ['depth', 'parentalias', 'alias', 'name', 'storagesize']
    with open(csv_filename, 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=csv_columns, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        for row in result_list:
            writer.writerow(row)


if __name__ == '__main__':
    # Commandline interface
    parser = argparse.ArgumentParser(description='Collect the storage usage for the dataverses in a csv table')
    parser.add_argument('server_url', help='the url of the dataverse instance')
    parser.add_argument('-k', '--api-key', dest='api_token',
                        help='the API key or token to use', required=True)
    parser.add_argument('-o', '--output-file', dest='output_filename', default='storage_usage.csv',
                        help='the file to write the output to')
    parser.add_argument('-d', '--depth', dest='max_depth', type=int, default=1,
                        help='the max depth of the hierarchy to traverse')
    parser.add_argument('-g', '--include-grand-total', dest='include_grand_total', type=bool, default=False,
                        help='whether to include the grand total, which almost doubles server processing time')
    args = parser.parse_args()
    # Actual work
    collect_storage_usage(args.api_token, args.server_url,
                          args.output_filename, args.max_depth, args.include_grand_total)
