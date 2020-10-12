import cgi
import os
import sys
import json
from csv import DictReader

import urllib3
import certifi
import requests
from time import sleep
import boto3 as boto3
import string
import json
import random
from datetime import datetime

#from util import load_json_from_s3, update_status_on_s3
from appeears.appeears_util import login, post, logout

data_bucket = "mosquito-data"
appeears_url = "https://lpdaacsvc.cr.usgs.gov/appeears/api/"

auth = ('mosquito2019', 'Malafr#1')

s3 = boto3.resource(
    's3')

def lambda_handler(event, context):

    print("event ", event)

#    if event.get('body'):
#        event = json.loads(event['body'])
#    else:
#        return dict(statusCode='200',
 #                   headers={'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*',
#                             'Access-Control-Allow-Headers': 'Content-Type',
#                             'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'},
#                    body=json.dumps({'message': "missing json parameters"}), isBase64Encoded='false')

    if 'body' in event:
        try:
            event = json.loads(event['body'])
        except (TypeError, ValueError):
            return dict(statusCode='200',
                        headers={'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*',
                                 'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
                                 'Access-Control-Allow-Methods': 'DELETE, GET, HEAD, OPTIONS, PATCH, POST, PUT'},
                        body=json.dumps({'message': "missing json parameters"}), isBase64Encoded='false')

    #    "dataset": "precipitation", "org_unit": "district", "agg_period": "daily", "start_date": "1998-08-21T17:38:27Z",
#    "end_date": "1998-09-21T17:38:27Z", "data_element_id": "fsdfrw345dsd"
    dataset = event['dataset']
    org_unit = event['org_unit']
    period = event['agg_period']
    start_date = event['start_date']
    end_date = event['end_date']
    data_element_id = event['data_element_id']
    boundaries = event['boundaries']
    request_id = ''.join(random.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits) for _ in range(10))
    # set some defaults
    #statType='median'
    statType='none'
    product='none'
    var_name='none'
    if "stat_type" in event:
        statType = event['stat_type']
    if "product" in event:
        product = event['product']
    if "var_name" in event:
        var_name = event['var_name']
    if "appeears_url" in event:
        appeears_url = event['appeears_url']
    if "auth_name" in event and "auth_pw" in event:
        auth = (event['auth_name'], event['auth_pw'])

    tasks = {}
    org_unit_id = {}
    # set up json task definitions using features defined by geotiff org boundaries in event payload
    for boundary in boundaries:
        geoJson = {}
        geoJson['type'] = 'FeatureCollection'
        features = []
        feature_entry = {}
        feature_entry['type'] = 'Feature'
        feature_entry['properties'] = {'name': boundary['name'], 'id': boundary['id']}
        feature_entry['geometry'] = boundary['geometry']
        features.append(feature_entry)
        geoJson['features'] = features
        org_unit_id[boundary['name']] = boundary['id']
        # set up tasks:
        tasks[boundary['name']] = {'task_type': 'area',
                                   'task_name': boundary['name'],
                                   'params': {'dates': [{'startDate': start_date, 'endDate': end_date}],
                                              'layers': [{'layer': var_name, 'product': product}],
                                              #                'output': {'format': {'type': 'netcdf4'}, 'projection': 'native'},
                                              'output': {'format': {'type': 'geotiff'}, 'projection': 'native'},
                                              'geo': geoJson}}

        print(tasks[boundary['name']])

    try:
        token = login(appeears_url,auth) # get new token
        # token = login()
        # task_id = submit_task('test_task',product, layer, start_date, end_date)

        # Post json to the API task service, return response as json
        hdrs = {'Content-Length': '0', 'Authorization': 'Bearer ' + token}
        # r = requests.post(appeears_url + 'logout', data={}, headers=hdrs)
        task_list = []
        for key, task in tasks.items():
            print("task ", task)
            #            task_response = requests.post(appeears_url +'task', json=task, headers=hdrs, timeout=10.0)
            task_response = post(appeears_url + 'task', task, hdrs, 30.0)
            # print("task response", task_response.json())
            #
            id = task_response.json()['task_id']
            entry = {"region":key, "task_id":id}
            task_list.append(entry)

        #resp = logout(appeears_url,token) # only call this to expire the token
    except Exception as e:
        print("Exception: ", e)
        return dict(statusCode='200', headers={'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*',
                                               'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
                                               'Access-Control-Allow-Methods': 'DELETE, GET, HEAD, OPTIONS, PATCH, POST, PUT'},
                    body=json.dumps({'message': "Error submitting appeears tasks "}), isBase64Encoded='false')

    print(task_list)

    # datetime object containing current date and time
    now = datetime.now()
    date_st = now.strftime("%m-%d-%YT%H:%M:%SZ")
    print("tasks started: " + date_st)

    # format new json structure
    task_return = {"appeears_token":token, "creation_time":date_st, "startDate": start_date, "endDate": end_date,
                   "product":product, "variable":var_name, "tasks":task_list}
    print(task_return)

    return dict(statusCode='200', headers={'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*',
                                           'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
                                           'Access-Control-Allow-Methods': 'DELETE, GET, HEAD, OPTIONS, PATCH, POST, PUT'},
                body=json.dumps(task_return), isBase64Encoded='false')

