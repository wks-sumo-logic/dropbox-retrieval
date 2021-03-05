#!/usr/bin/env python3

"""
Simple script to retrieve Dropbox data
"""

import os
import sys
import json
import datetime
import logging
import requests

logging.basicConfig(level=logging.INFO)

DROPBOX_BASE_DIR = '/var/lock/dropbox'

DROPBOX_LOCK_DIR = os.path.join(DROPBOX_BASE_DIR, 'lock')
DROPBOX_LOCK_FILE = os.path.join(DROPBOX_LOCK_DIR, 'dropbox-timestamp.lock')
logging.info('lock_directory: %s', DROPBOX_LOCK_FILE)

DROPBOX_LOGS_DIR = os.path.join(DROPBOX_BASE_DIR, 'logs')
DROPBOX_LOGS_FILE = os.path.join(DROPBOX_LOGS_DIR, 'dropbox-timestamp.log')
logging.info('logs_directory: %s', DROPBOX_LOGS_FILE)

TODAY = datetime.datetime.today()

TIMESTAMP = "{:04d}-{:02d}-{:02d}T00:00:00.000".format(TODAY.year, TODAY.month, TODAY.day)

DROPBOX_BASE_URL = 'https://api.dropboxapi.com/2/team_log/get_events'

def get_timestamp_data():
    """
    Collect the timestamp from either a local file or current date: DROPBOX_LOCK_FILE
    """

    for target_dir in ( DROPBOX_LOCK_DIR, DROPBOX_LOGS_DIR ):
        if not os.path.exists(target_dir):
            logging.info('creating_directory: %s', target_dir)
            os.makedirs(target_dir)

    my_timestamp = TIMESTAMP

    if os.path.exists(DROPBOX_LOCK_FILE):
        logging.info('reading_timestamp: %s', DROPBOX_LOCK_FILE)
        with open(DROPBOX_LOCK_FILE, 'r') as time_stamp_file:
            my_timestamp = time_stamp_file.read()

    return my_timestamp + 'Z', DROPBOX_LOCK_FILE

def put_timestamp_data(my_tsvalue, my_tsfile):
    """
    Persist the timestamp into a local file for comparison: DROPBOX_LOCK_FILE
    """

    my_timestamp = my_tsvalue.replace(microsecond=0).isoformat()
    try:
        with open(my_tsfile, 'w') as time_stamp_file:
            logging.info('writing_timestamp: %s', DROPBOX_LOCK_FILE)
            time_stamp_file.write(my_timestamp)
    except OSError as my_error:
        logging.info('Unexpected error (%d): %s', my_error.errno, my_error.strerror)
        raise
    return my_timestamp + 'Z'

def remove_dot_key(json_object):
    """
    Clean up Json object: json_object
    """

    for key in json_object.keys():
        new_key = json_object.replace(".","")
        if new_key != key:
            json_object[new_key] = json_object[key]
            del json_object[key]
    return json_object

if __name__ == '__main__':

    timestamp, dropboxLockFile = get_timestamp_data()

    now = put_timestamp_data(datetime.datetime.now(), dropboxLockFile)

    #  --data "{\"time\": { \"start_time\": \"2018-4-13T14:00:00Z\" }}"

    start_end_time = { 'start_time': timestamp, 'end_time': now }
    jsonData= { 'time': start_end_time }

    DROPBOX_TARGET_URL = DROPBOX_BASE_URL

    TOTAL_EVENTS = 0

    CONTINUE_FLAG = "no"

    GET_DATA = "true"

    while GET_DATA == "true":

        header_dict = {}

        header_dict['Accept'] = 'application/json'
        header_dict['Content-Type'] = 'application/json'
        header_dict['Authorization'] = 'Bearer <Enter your access token here>'

        get_response = requests.post(DROPBOX_TARGET_URL,headers=header_dict)
        my_status = get_response.status_code
        my_payload = get_response.content

        if get_response.status_code != 200:
            logging.error('status_code: %d - %s', my_status, my_payload)
            sys.exit(get_response.status_code)
        else:
            dropbox_json_logs = json.loads(get_response.content, object_hook=remove_dot_key)
        events = dropbox_json_logs['events']

        events_size = len(events)
        TOTAL_EVENTS += events_size

        output_file = open(DROPBOX_LOGS_FILE, 'a+')
        for event in events:
            jsonLog = json.dumps(event)
            output_file.write('{}\n'.format(jsonLog))

        if dropbox_json_logs['has_more'] == 'true':
            DROPBOX_TARGET_URL = 'https://api.dropboxapi.com/2/team_log/get_events/continue'
            jsonData = { 'cursor': dropbox_json_logs['cursor'] }
            logging.info('Retrieved: %d bytes and getting more data.', events_size)
        else:
            GET_DATA = "false"
