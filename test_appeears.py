import cgi
import os
import json
from csv import DictReader

import requests
from time import sleep

start_url = "https://8rgnpf3jfd.execute-api.us-east-1.amazonaws.com/default/start_appeears_tasks"
status_url = "https://o1w0q7e4f8.execute-api.us-east-1.amazonaws.com/default/status_appeears_tasks"
download_url = "https://ny74dq5nri.execute-api.us-east-1.amazonaws.com/default/download_appeears_results"

def post(url, json_payload, hdrs, timeout):
    task_response=requests.post(url, json=json_payload, headers=hdrs, timeout=timeout)
    task_response.raise_for_status()
    return task_response

def get(url, hdrs, timeout):
    task_response = requests.get(url, headers=hdrs, timeout=timeout)
    task_response.raise_for_status()
    return task_response


def main():

    payload = "appeears_payload_MOD11A2.json"

    with open(payload) as f:
        jsonData = json.load(f)
    f.close()

    try:

        # Post json to the API task service, return response as json
        hdrs = {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'}
        print("Submitting AppEEARS order...")

        task_response = post(start_url, jsonData, hdrs, 120.0)
        # print("task response", task_response.json())
        resp = task_response.json()

        #resp = {'appeears_token': 'eIIhCKpiRYB6RPo2YtWhKSGCtZt-1W47USBv8FKY1NNSMNkyUxeIStL0Oyon-pDgNHp4qIgn2t6hTi86j_gEwQ', 'appeears_url': 'https://lpdaacsvc.cr.usgs.gov/appeears/api/', 'creation_time': '10-14-2020T17:49:48Z', 'startDate': '2018-03-01T00:00:00.000Z', 'endDate': '2018-03-31T00:00:00.000Z', 'product': 'MOD11A2.006', 'variable': 'LST_Day_1km', 'data_element_id': '8675309', 'tasks': [{'name': 'Bo', 'org_unit_id': 'O6uvpzGd5pu', 'task_id': '91aac4ce-0b0d-4d72-8247-98fa19788a41'}, {'name': 'Bombali', 'org_unit_id': 'fdc6uOvgoji', 'task_id': '84dea589-bf55-46f5-99fc-921e4ff4faab'}, {'name': 'Bonthe', 'org_unit_id': 'lc3eMKXaEfw', 'task_id': '4d976fc4-4ce1-4fd9-83d0-a1f0e7f710c3'}, {'name': 'Kailahun', 'org_unit_id': 'jUb8gELQApl', 'task_id': 'e1f1dc1c-4507-4e96-ab45-6f4bae68d69e'}, {'name': 'Kambia', 'org_unit_id': 'PMa2VCrupOd', 'task_id': '30135603-c56f-436d-b155-918613aeeb56'}, {'name': 'Kenema', 'org_unit_id': 'kJq2mPyFEHo', 'task_id': '00ce3fe7-a986-4d30-b8dd-a173c30c8ff8'}, {'name': 'Koinadugu', 'org_unit_id': 'qhqAxPSTUXp', 'task_id': 'c98b6b46-aa5e-42cb-9a0d-fcb5564ba6d3'}, {'name': 'Kono', 'org_unit_id': 'Vth0fbpFcsO', 'task_id': '6ea8839e-9411-4569-b484-1990ecab45ff'}, {'name': 'Moyamba', 'org_unit_id': 'jmIPBj66vD6', 'task_id': 'c2e36531-ff12-43e6-b33c-2c17d244629b'}, {'name': 'Port Loko', 'org_unit_id': 'TEQlaapDQoK', 'task_id': '7fc66dd5-7ac9-4c4f-9d1e-f05ccc915b76'}, {'name': 'Pujehun', 'org_unit_id': 'bL4ooGhyHRQ', 'task_id': '2b85f759-ac1c-4fd8-8238-dad03a85f865'}, {'name': 'Tonkolili', 'org_unit_id': 'eIQbndfxQMb', 'task_id': '65252d50-fdfa-4e95-bed6-ae1ba645ec0c'}, {'name': 'Western Area', 'org_unit_id': 'at6UHUQatSo', 'task_id': '002b3659-6a5b-4a04-a01c-29b19bb5c8b7'}]}
        #resp = {'appeears_token': 'eIIhCKpiRYB6RPo2YtWhKSGCtZt-1W47USBv8FKY1NNSMNkyUxeIStL0Oyon-pDgNHp4qIgn2t6hTi86j_gEwQ', 'appeears_url': 'https://lpdaacsvc.cr.usgs.gov/appeears/api/', 'creation_time': '10-14-2020T21:25:28Z', 'startDate': '2018-03-01T00:00:00.000Z', 'endDate': '2018-03-31T00:00:00.000Z', 'product': 'MOD11A2.006', 'variable': 'LST_Day_1km', 'data_element_id': '8675309', 'tasks': [{'name': 'Bo', 'org_unit_id': 'O6uvpzGd5pu', 'task_id': 'a40fcaa2-d2b3-44db-8538-ae105ec4ae0a'}, {'name': 'Bombali', 'org_unit_id': 'fdc6uOvgoji', 'task_id': '3dbc6fd7-f98c-4aa8-90fb-05b73421c650'}, {'name': 'Bonthe', 'org_unit_id': 'lc3eMKXaEfw', 'task_id': 'c0d7beb7-a2c9-4401-9448-54392e5dd021'}, {'name': 'Kailahun', 'org_unit_id': 'jUb8gELQApl', 'task_id': 'b00a933c-221e-4159-b7cc-dce77ec20142'}, {'name': 'Kambia', 'org_unit_id': 'PMa2VCrupOd', 'task_id': '91446654-8ad9-4cf9-ba33-0420eadc1681'}, {'name': 'Kenema', 'org_unit_id': 'kJq2mPyFEHo', 'task_id': '0b81aa56-dbd6-46dc-ae0f-98c6eced0792'}, {'name': 'Koinadugu', 'org_unit_id': 'qhqAxPSTUXp', 'task_id': '5635b09d-36e5-4200-9200-12a7da17c403'}, {'name': 'Kono', 'org_unit_id': 'Vth0fbpFcsO', 'task_id': 'c2f65890-2253-4075-8844-4a73470342e9'}, {'name': 'Moyamba', 'org_unit_id': 'jmIPBj66vD6', 'task_id': '7d879683-2db5-4c77-8bb1-3800a901cb69'}, {'name': 'Port Loko', 'org_unit_id': 'TEQlaapDQoK', 'task_id': 'f8335461-165e-42c2-9e8b-dbedf620471e'}, {'name': 'Pujehun', 'org_unit_id': 'bL4ooGhyHRQ', 'task_id': 'ae061c48-4c31-476e-a36a-d501998854f6'}, {'name': 'Tonkolili', 'org_unit_id': 'eIQbndfxQMb', 'task_id': '55001179-d79b-423e-904f-a309d23abee9'}, {'name': 'Western Area', 'org_unit_id': 'at6UHUQatSo', 'task_id': '4cd8a04d-cb73-4f3f-801e-16457c68f890'}]}

        if 'error' in resp.keys():
            print("error starting AppEEARS order: ", resp['error'])
            exit(-1)
        print(resp)
        tasks = resp['tasks']
        token = resp['appeears_token']
        appeears_url = resp['appeears_url']
        data_element_id = resp['data_element_id']
        product = resp['product']

        #setup status_appeears_tasks json payload
        statusTask = {'appeears_url': appeears_url, 'appeears_token': token, 'tasks': tasks}
        print("Waiting for AppEEARS orders to finish...")
        # check status in loop
        count = 0
        while True:
            task_response = post(status_url, statusTask, hdrs, 120.0)
            # print("task response", task_response.json())
            statusJson = task_response.json()
            if 'error' in statusJson.keys():
                print("error checking AppEEARS order status: ", statusJson['error'])
                exit(-1)
            print(statusJson)

            #{'task_id': id, 'name': name, 'org_unit_id': org_unit_id, 'status': status}
            #req = requests.get(appeears_url + 'task/' + task_id, headers=hdrs)
            found_all = True
            for task in statusJson['task_list']:
                if task['status']=='done':
                    continue
                else:
                    found_all = False
                    break
            if found_all==True:
                print("AppEEARS orders finished")
                break
            sleep(5)
            count = count+1
            print("count ", count)
            if count > 100:
                print("request timed out ")
                break;

        # token = event['appeears_token']
        # data_element_id = event['data_element_id']
        # product = event['product']
        # tasks = event['tasks']
        # if "appeears_url" in event:
        #     appeears_url = event['appeears_url']
        # for task in tasks:
        #     name = task['name']
        #     org_unit_id = task['org_unit_id']
        #     task_id = task['task_id']
        # jsonRecord = {'dataElement': data_element_id, 'period': dateStr,
        #               'orgUnit': org_unit_id, 'value': value}

        download_data = {'appeears_token':token, 'appeears_url':appeears_url, 'data_element_id':data_element_id,'product':product,'tasks':tasks}
        downloadResp = post(download_url, download_data, hdrs, 120.0)
        outputJson = downloadResp.json()
        if 'error' in outputJson.keys():
            print("error downloading AppEEARS results: ", outputJson['error'])
            exit(-1)
        print(outputJson)
            # process csv file and create output records, if csv stats file not found, data is missing
        print("AppEEARS Download complete!")

        #resp = logout(token)
    except Exception as e:
        print("Exception: ",e)

if __name__ == '__main__':
   main()
