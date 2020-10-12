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
from appeears.appeears_util import get, post

appeears_url = "https://lpdaacsvc.cr.usgs.gov/appeears/api/"

s3 = boto3.resource(
    's3')

def lambda_handler(event, context):

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
    token = event['appeears_token']
    tasks = event['tasks']
    if "appeears_url" in event:
        appeears_url = event['appeears_url']

    try:

        # Post json to the API task service, return response as json
        hdrs = {'Content-Length': '0', 'Authorization': 'Bearer ' + token}
        # r = requests.post(appeears_url + 'logout', data={}, headers=hdrs)

        id_status=[]
        for task in tasks:
            id = task['task_id']
            name = task['name']
            org_unit_id = task['org_unit_id']

            # req = requests.get(appeears_url + 'task/' + id, headers=hdrs, timeout=10.0)
            req = get(appeears_url + 'task/' + id, hdrs, 30.0)
            # print("request ",req)
            status = req.json()['status']
            id_status.append({'task_id':id,'name':name, 'org_unit_id':org_unit_id,'status':status})
            # print("count ", count, " status: ",status)
            # if status == 'done':
            #     task_status[id] = True
            #     print("task ", task_id_to_name[id], " done")
            #     # break;
            # else:  # at least one job not done, stop checking status
            #     break
    except Exception as e:
        print("Exception: ", e)
        return dict(statusCode='200', headers={'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*',
                                               'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
                                               'Access-Control-Allow-Methods': 'DELETE, GET, HEAD, OPTIONS, PATCH, POST, PUT'},
                    body=json.dumps({'message': "Error checking appeears task status "}), isBase64Encoded='false')

    print(id_status)

    return dict(statusCode='200', headers={'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*',
                                           'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
                                           'Access-Control-Allow-Methods': 'DELETE, GET, HEAD, OPTIONS, PATCH, POST, PUT'},
                body=json.dumps(id_status), isBase64Encoded='false')

