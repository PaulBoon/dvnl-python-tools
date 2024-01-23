#!/usr/bin/env python3

###
# prerequisites:
#   install the requests module:
#   python3 -m pip install requests
#
import requests

import argparse
import csv
import re
import time

# The dataverse tree (hierarchy of sub-verses) is public metrics,
# so only published info can be retrieved.
def get_tree(server_url):
    url = server_url + "/api/info/metrics/tree"
    dv_resp = requests.get(url)
    resp_data = dv_resp.json()['data']
    return resp_data


# TODO get info, permission stuff
def get_group_info(api_token, server_url, alias):
    time.sleep(0.3)  # BE NICE?
    headers = {'X-Dataverse-key': api_token}
    url = server_url + "/api/dataverses/" + alias + "/groups"
    dv_resp = requests.get(url, headers=headers)
    # print(dv_resp.json())
    resp_data = dv_resp.json()['data']
    # print(resp_data['message'])
    # return resp_data['message']
    # flatten and compact it... no list comprehension though
    result_list = []
    for group in resp_data:
        #  append the number of assignees in braces
        result_list.append(group['identifier'] + ' (' + str(len(group['containedRoleAssignees'])) + ')')
    return ', '.join(result_list)

def get_role_info(api_token, server_url, alias):
    time.sleep(0.3)  # BE NICE?
    headers = {'X-Dataverse-key': api_token}
    url = server_url + "/api/dataverses/" + alias + "/roles"
    dv_resp = requests.get(url, headers=headers)
    # print(dv_resp.json())
    resp_data = dv_resp.json()['data']
    # print(resp_data['message'])
    # return resp_data['message']
    # flatten and compact it... no list comprehension though
    result_list = []
    for role in resp_data:
        #  append the number of permissions in braces
        result_list.append(role['alias'] + ' (' + str(len(role['permissions'])) + ')')
    return ', '.join(result_list)

def get_assignment_info(api_token, server_url, alias):
    time.sleep(0.3)  # BE NICE?
    headers = {'X-Dataverse-key': api_token}
    url = server_url + "/api/dataverses/" + alias + "/assignments"
    dv_resp = requests.get(url, headers=headers)
    # print(dv_resp.json())
    resp_data = dv_resp.json()['data']
    # print(resp_data['message'])
    # return resp_data['message']
    # flatten and compact it... no list comprehension though
    result_list = []
    for assignment in resp_data:
        #  append the role alias in braces
        result_list.append(assignment['assignee'] + ' (' + (assignment['_roleAlias']) + ')')
    return ', '.join(result_list)

# Traverses the tree and collect info for ech dataverse using recursion.
def get_verse_info(api_token, server_url, tree_data, parent_vpath, parent_alias, depth=1):
    result_list = []
    alias = tree_data['alias']
    name = tree_data['name']
    id = tree_data['id']
    vpath = parent_vpath + '/' + alias
    print("Retrieving info for this dataverse instance, virtual path: {}".format(vpath))
    group_info = get_group_info(api_token, server_url, alias)
    role_info = get_role_info(api_token, server_url, alias)
    assignment_info = get_assignment_info(api_token, server_url, alias)

    row = {'vpath': vpath, 'url': server_url + '/' + alias, 'depth': depth, 'parentalias': parent_alias, 'alias': alias,
           'name': name, 'id': id, 'groups': group_info, 'roles': role_info, 'assignments': assignment_info}
    result_list.append(row)
    # only direct descendants (children)
    if 'children' in tree_data:
        for child_tree_data in tree_data['children']:
            result_list.extend(get_verse_info(api_token, server_url, child_tree_data, vpath, alias, depth + 1))  # recurse
    return result_list

def find_verse(verses , alias):
    for verse in verses:
        if verse["alias"] == alias:
            return verse

def collect_permissions_overview(api_token, server_url, output_filename):
    print("Start collecting permissions overview for: " + args.server_url)

    # construct the result dictionary
    result_list = []

    tree_data = get_tree(server_url)

    alias = tree_data['alias']
    name = tree_data['name']
    print("Extracted the tree for the toplevel dataverse: {} ({})".format(name, alias))

    vpath = '/' + alias
    print("Retrieving info for this dataverse instance")
    group_info = get_group_info(api_token, server_url, alias)
    role_info = get_role_info(api_token, server_url, alias)
    assignment_info = get_assignment_info(api_token, server_url, alias)
    id = tree_data['id']

    row = {'vpath': vpath, 'url': server_url + '/' + alias, 'depth': 0, 'parentalias': alias, 'alias': alias,
           'name': name, 'id': id, 'groups': group_info, 'roles': role_info, 'assignments': assignment_info}
    result_list.append(row)

    # TEST, restrict to only a specific subverse
    child_tree_data = find_verse(tree_data['children'], 'tilburg')
    result_list.extend(get_verse_info(api_token, server_url, child_tree_data, vpath, alias, 1))


    # store it as csv
    csv_filename = output_filename
    # might be nice to add timestamp or maybe a counter to the filename if it already exists
    csv_columns = ['vpath', 'url', 'depth', 'parentalias', 'alias', 'name', 'id', 'groups', 'roles', 'assignments']
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
    args = parser.parse_args()
    # Actual work
    collect_permissions_overview(args.api_token, args.server_url,
                                 args.output_filename)
