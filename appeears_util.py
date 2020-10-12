import json
import botocore

# This method POSTs formatted JSON WSP requests to the GES DISC endpoint URL
# It is created for convenience since this task will be repeated more than once
import requests

def login(url,auth):
    hdrs = {'Content-Length': '0'}
    r = requests.post(url+'login', data={}, auth=auth, headers=hdrs)
    response = json.loads(r.text)
    print('login response ', response)
    if 'token' not in response:
        if 'message' in response:
            except_msg = response['message']
        else:
            except_msg = "Login errror: unknown failure"
        print("Login failed:")
        raise Exception(except_msg)
    print("Successfully logged in")
    return response['token']

def logout(url,token):
    hdrs = {'Content-Length': '0', 'Authorization': 'Bearer '+token}
    r = requests.post(url+'logout', data={}, headers=hdrs)
    print('logout status ', r.status_code)
    if r.status_code != 204:
        print("Logout failed:")
        raise Exception("logout error: status code "+str(r.status_code))
    else:
        print("Successfully logged out")
    # Check for errors
    # if response['type'] == 'jsonwsp/fault':
    #     print('API Error: faulty %s request' % response['methodname'])
    #     sys.exit(1)
    return r.status_code

def post(url, json_payload, hdrs, timeout):
    task_response=requests.post(url, json=json_payload, headers=hdrs, timeout=timeout)
    task_response.raise_for_status()
    return task_response

def get(url, hdrs, timeout):
    task_response = requests.get(url, headers=hdrs, timeout=timeout)
    task_response.raise_for_status()
    return task_response



def load_json_from_s3(bucket, key):

    print("event key " + key)
    # strip off directory from key for temp file
    key_split = key.split('/')
    download_fn=key_split[len(key_split) - 1]
    file = "/tmp/" + download_fn
    
    try:
        bucket.download_file(key, file)
    except botocore.exceptions.ClientError as e:
        print("Error reading the s3 object " + key)
        jsonData = {"message": "error"}
        return jsonData

    try:
        with open(file) as f:
            jsonData = json.load(f)
        f.close()
    except IOError:
        print("Could not read file:" + file)
        jsonData = {"message": "error"}

    return jsonData

def update_status_on_s3(bucket, request_id, type, status, message, **kwargs):
    statusJson = {"request_id": request_id, "type": type, "status": status, "message": message}
    for key, value in kwargs.items():
        statusJson[key] = value
    with open("/tmp/" + request_id + "_"+ type +".json", 'w') as status_file:
        json.dump(statusJson, status_file)
    #        json.dump(districtPrecipStats, json_file)
    status_file.close()

#    bucket.upload_file("/tmp/" + request_id + "_" + type +".json",
#                                       "status/" + request_id + "_" + type +".json")
    bucket.upload_file("/tmp/" + request_id + "_" + type +".json",
                                       "status/" + request_id + ".json")
