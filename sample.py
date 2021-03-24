import json
import csv
import boto3
import os
import datetime as dt
import re
import requests
from requests_aws4auth import AWS4Auth


region = 'us-east-2' # e.g. us-west-1
service = 'es'
#credentials = boto3.Session().get_credentials()
#awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

host = 'https://search-csv2jsonpoc-hb5nfchrkc7e7fg5idsocdgily.us-east-2.es.amazonaws.com' # the Amazon ES domain, including https://
index = 'lambda-s3-index'
type = 'lambda-type'
url = host + '/' + index + '/' + type

headers = { "Content-Type": "application/json" }

s3 = boto3.client('s3')

street_pattern = re.compile('\"(.+)\"')
city_pattern = re.compile('\"(.+)\"')

def handler(event, context):
    datestamp = dt.datetime.now().strftime("%Y/%m/%d")
    timestamp = dt.datetime.now().strftime("%s")

    filename_json = "/tmp/file_{ts}.json".format(ts=timestamp)
    filename_csv = "/tmp/file_{ts}.csv".format(ts=timestamp)
    keyname_s3 = "uploads/output/output.json".format(ts=timestamp)

    json_data = []

    for record in event["Records"]:
        bucket_name = record["s3"]["bucket"]["name"]
        key_name = record["s3"]["object"]["key"]

    s3_object = s3.get_object(Bucket=bucket_name, Key=key_name)
    data = s3_object['Body'].read()
    contents = data.decode('utf-8')

    with open(filename_csv, 'a') as csv_data:
        csv_data.write(contents)

    with open(filename_csv) as csv_data:
        csv_reader = csv.DictReader(csv_data)
        for csv_row in csv_reader:
            json_data.append(csv_row)

    with open(filename_json, 'w') as json_file:
        json_file.write(json.dumps(json_data))

    with open(filename_json, 'r') as json_file_contents:
        response = s3.put_object(Bucket=bucket_name, Key=keyname_s3, Body=json_file_contents.read())
    
    
    file = s3.get_object(Bucket=bucket_name, Key=keyname_s3)
    body = file['Body'].read()
    c = body.decode('utf-8')
    lines = c.splitlines()
        
        # Match the regular expressions to each line and index the JSON
    for line in lines:
        street = street_pattern.search(line).group(1)
        city = city_pattern.search(line).group(1)
            
            
        document = { "street": street, "city": city }
        o = requests.post(url, json=document, headers=headers)
        print("result : ",o)
   # out = requests.post(url, auth=awsauth, json=json_file_contents.read(), headers=headers)
   

   # os.remove(filename_csv)
   # os.remove(filename_json)