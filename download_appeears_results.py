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

this_api = "https://8rgnpf3jfd.execute-api.us-east-1.amazonaws.com/default/start_appeears_tasks"
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
    data_element_id = event['data_element_id']
    product = event['product']
    tasks = event['tasks']
    if "appeears_url" in event:
        appeears_url = event['appeears_url']

    # Post json to the API task service, return response as json
    hdrs = {'Content-Length': '0', 'Authorization': 'Bearer ' + token}

    try:

        outputJson = []

        # download product stat file out of bundles
        csv_file = product.replace('.', '-') + '-Statistics.csv'
        for task in tasks:
            name = task['name']
            org_unit_id = task['org_unit_id']
            task_id = task['task_id']
            print("downloading ", name)

            download_response = requests.get(appeears_url + 'bundle/' + task_id + '/' + csv_file,
                                                 stream=True)  # Get a stream to the bundle file
            download_response.raise_for_status()
            filename = os.path.basename(cgi.parse_header(download_response.headers['Content-Disposition'])[1][
                                                'filename'])  # Parse the name from Content-Disposition header
            with open(filename, 'wb') as fp:  # Write file to dest dir
                for data in download_response.iter_content(chunk_size=8192):
                    fp.write(data)

            # parse out csv file
            with open(filename, 'r') as read_obj:
                # pass the file object to DictReader() to get the DictReader object
                csv_dict_reader = DictReader(read_obj)
                # iterate over each line as a ordered dictionary
                # for row in csv_dict_reader:
                #     # row variable is a dictionary that represents a row in csv
                #     print(row)
                # column_names = csv_dict_reader.fieldnames
                # print(column_names)
                # structure of CSV file
                # File Name,Dataset,aid,Date,Count,Minimum,Maximum,Range,Mean,Standard Deviation,Variance,Upper Quartile,Upper 1.5 IQR,Median,Lower 1.5 IQR,Lower Quartile
                # MOD11A2_006_LST_Day_1km_doy2018057_aid0001,LST_Day_1km,aid0001,2018-02-26,6803.0,297.38,311.6,"(297.38,311.6)",303.7146,1.7401,3.028,304.87,308.44,303.74,298.9,302.48
                # MOD11A2_006_LST_Day_1km_doy2018065_aid0001,LST_Day_1km,aid0001,2018-03-06,6807.0,296.16,310.72,"(296.16,310.72)",301.8998,1.796,3.2256,303.04,306.8,302.02,296.86,300.52
                # MOD11A2_006_LST_Day_1km_doy2018073_aid0001,LST_Day_1km,aid0001,2018-03-14,5213.0,295.9,310.22,"(295.9,310.22)",303.2248,1.9724,3.8905,304.78,308.98,303.08,297.78,301.98
                # MOD11A2_006_LST_Day_1km_doy2018081_aid0001,LST_Day_1km,aid0001,2018-03-22,1685.0,292.6,304.8,"(292.6,304.8)",299.7435,1.7759,3.1537,301.06,304.22,299.68,295.14,298.66
                # MOD11A2_006_LST_Day_1km_doy2018089_aid0001,LST_Day_1km,aid0001,2018-03-30,3423.0,296.82,306.86,"(296.82,306.86)",300.2546,1.2143,1.4745,300.88,302.98,300.16,297.4,299.48
                # append statistics to json
                for row in csv_dict_reader:
                    value = row['Mean']
                    dateStr = row['Date'].replace('-', '')
                    # dateStr = startTime.strftime("%Y%m%d")
                    jsonRecord = {'dataElement': data_element_id, 'period': dateStr,
                                  'orgUnit': org_unit_id, 'value': value}
                    outputJson.append(jsonRecord)

        print(outputJson)

        print("Downloading complete!")

    except Exception as e:
        print("Exception: ", e)
        return dict(statusCode='200', headers={'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*',
                                               'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
                                               'Access-Control-Allow-Methods': 'DELETE, GET, HEAD, OPTIONS, PATCH, POST, PUT'},
                    body=json.dumps({'message': "Error downloading appeears results "}), isBase64Encoded='false')

    #print(task_list)


    return dict(statusCode='200', headers={'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*',
                                           'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
                                           'Access-Control-Allow-Methods': 'DELETE, GET, HEAD, OPTIONS, PATCH, POST, PUT'},
                body=json.dumps(outputJson), isBase64Encoded='false')

