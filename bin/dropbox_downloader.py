#!/usr/bin/env python3

"""
dropbox_downloader - Simple script to retrieve Dropbox data

script flow:
    + evaluate/validate variables
    + create directory and files if needed
    + gets data and enriches it
    + calculates a checksum on enriched data
    + skips persisting data it has already "seen"
    + persists the data locally
"""

import os
import sys
import json
import datetime
import hashlib
import logging
import argparse
import configparser
import requests
import boto3

logging.basicConfig(level=logging.INFO)

PARSER = argparse.ArgumentParser(description="""
Collect data from dropbox via API and cache data locally
""")

PARSER.add_argument("-t", metavar='<token>', dest='MY_BEARER_TOKEN', \
                    help="set token")

PARSER.add_argument("-r", metavar='<start>', dest='MY_TIME_RANGE', \
                    default='1d', help="set time range from current date")

PARSER.add_argument("-s", metavar='<timestamps>', dest='MY_TIME_STAMPS', \
                    help="use time stamps to search")

PARSER.add_argument("-d", metavar='<cachedir>', dest='MY_CACHE_DIR', \
                    help="set directory")

PARSER.add_argument("-c", metavar='<cfgfile>', dest='MY_CFG_FILE', \
                    help="use config file")

PARSER.add_argument("-v", type=int, default=0, metavar='<verbose>', \
                    dest='VERBOSE', help="increase verbosity")

PARSER.add_argument("-i", "--initialize", action='store_true', default=False, \
                    dest='INITIALIZE', help="initialize config file")

ARGS = PARSER.parse_args()

TIME_RANGE = 0
DROPBOX_BASE_DIR = '/var/tmp/dropbox'
BEARER_TOKEN = 'UNSET'

def initialize_config_file():
    """
    Initialize configuration file, write output, and then exit
    """

    starter_config='/var/tmp/dropbox.initial.cfg'
    config = configparser.RawConfigParser()
    config.optionxform = str

    config.add_section('Default')

    cached_input = ( input ("Please enter your Cache Directory: \n") or DROPBOX_BASE_DIR )
    config.set('Default', 'CACHE_DIR', cached_input )

    token_input = input ("Please enter your Bearer Token: \n")
    config.set('Default', 'BEARER_TOKEN', token_input )

    time_range_input = ( input ("Please enter the desired time range: \n") or "1d" )
    config.set('Default', 'TIME_RANGE', time_range_input )

    with open(starter_config, 'w') as configfile:
        config.write(configfile)
    print('Complete! Written: {}'.format(starter_config))
    sys.exit()

if ARGS.INITIALIZE:
    initialize_config_file()

if ARGS.MY_CFG_FILE:

    CFGFILE = os.path.abspath(ARGS.MY_CFG_FILE)
    CONFIG = configparser.ConfigParser()
    CONFIG.read(CFGFILE)

    if CONFIG.has_option('Default', 'BEARER_TOKEN'):
        BEARER_TOKEN = CONFIG.get("Default", "BEARER_TOKEN")

    if CONFIG.has_option('Default', 'CACHE_DIR'):
        DROPBOX_BASE_DIR = os.path.abspath(CONFIG.get("Default", "CACHE_DIR"))

    if CONFIG.has_option('Default', 'TIME_RANGE'):
        TIME_RANGE = CONFIG.get("Default", "TIME_RANGE")

if ARGS.MY_BEARER_TOKEN:
    BEARER_TOKEN = ARGS.MY_BEARER_TOKEN

if ARGS.MY_CACHE_DIR:
    DROPBOX_BASE_DIR = os.path.abspath(ARGS.MY_CACHE_DIR)

if ARGS.MY_TIME_RANGE:
    TIME_RANGE = ARGS.MY_TIME_RANGE

if BEARER_TOKEN == "UNSET":
    logging.error('BEARER TOKEN unset. Exiting.')
    sys.exit(10)

if ARGS.VERBOSE > 5:
    print('Token: {}'.format(BEARER_TOKEN))
    print('Cache: {}'.format(DROPBOX_BASE_DIR))
    print('Range: {}'.format(TIME_RANGE))

DATE_STAMP = datetime.datetime.now().strftime('%Y%m%d')

DROPBOX_LOGS_DIR = os.path.join(DROPBOX_BASE_DIR, 'logs')
DROPBOX_LOGS_NAME = 'dropbox-downloads.' + DATE_STAMP + '.log'
DROPBOX_LOGS_FILE = os.path.join(DROPBOX_LOGS_DIR, DROPBOX_LOGS_NAME)

if ARGS.VERBOSE > 3:
    print('resolving logs_directory: {}'.format(DROPBOX_LOGS_FILE))

DROPBOX_SUMS_DIR = os.path.join(DROPBOX_BASE_DIR, 'sums')
DROPBOX_SUMS_NAME = 'dropbox-checksums.' + DATE_STAMP + '.sum'
DROPBOX_SUMS_FILE = os.path.join(DROPBOX_SUMS_DIR, DROPBOX_SUMS_NAME)

if ARGS.VERBOSE > 3:
    print('resolving logs_directory: {}'.format(DROPBOX_SUMS_FILE))

TODAY = datetime.datetime.today()

TIMESTAMP = "{:04d}-{:02d}-{:02d}T00:00:00.000".format(TODAY.year, TODAY.month, TODAY.day)

DROPBOX_BASE_URL = 'https://api.dropboxapi.com/2/team_log/get_events'

SECONDS_TABLE = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}

for target_dir in ( DROPBOX_LOGS_DIR, DROPBOX_SUMS_DIR ):
    if not os.path.exists(target_dir):
        if ARGS.VERBOSE > 5:
            print('creating_directory: {}'.format(target_dir))
        os.makedirs(target_dir)

def convert_to_seconds(my_range):
    """
    Convert the range string into seconds
    """
    return int(my_range[:-1]) * SECONDS_TABLE[my_range[-1]]

if __name__ == '__main__':

    my_now = datetime.datetime.now()

    if ARGS.MY_TIME_STAMPS:
        START_DATE, FINAL_DATE = ARGS.MY_TIME_STAMPS.split('#')
    else:
        final_seconds = int( my_now.timestamp() )
        start_seconds = final_seconds - ( convert_to_seconds(TIME_RANGE) )
        delta_seconds = convert_to_seconds(TIME_RANGE)

        FINAL_DATE = datetime.datetime.fromtimestamp(final_seconds)
        FINAL_DATE = ( str ( FINAL_DATE.strftime("%Y-%m-%dT%H:%M:%S") )  + 'Z' )

        START_DATE = datetime.datetime.fromtimestamp(start_seconds)
        START_DATE = ( str ( START_DATE.strftime("%Y-%m-%dT%H:%M:%S") )  + 'Z' )

    if ARGS.VERBOSE > 5:

        print('START_DATE: {}'.format(START_DATE))
        print('FINAL_DATE: {}'.format(FINAL_DATE))

        if not ARGS.MY_TIME_STAMPS:
            print('START_SECONDS: {}'.format(start_seconds))
            print('FINAL_SECONDS: {}'.format(final_seconds))
            print('DELTA_SECONDS: {}'.format(delta_seconds))

    start_end_time = { 'start_time': START_DATE, 'end_time': FINAL_DATE }
    json_data= { 'time': start_end_time }

    DROPBOX_TARGET_URL = DROPBOX_BASE_URL

    TOTAL_EVENTS = 0

    CONTINUE_FLAG = "no"

    GET_DATA = "true"

    if "aws:ssm:" in BEARER_TOKEN:
        VENDOR, METHOD, REGION, TOKENS = BEARER_TOKEN.split(':')
        if ARGS.VERBOSE > 7:
            print('VENDOR: {}'.format(VENDOR))
            print('METHOD: {}'.format(METHOD))
            print('REGION: {}'.format(REGION))
            print('TOKENS: {}'.format(TOKENS))

        ssmobject = boto3.client(METHOD, region_name=REGION)
        ssmresponse = ssmobject.get_parameters(
            Names=[ TOKENS ],
            WithDecryption=True
        )

        BEARER_TOKEN = ssmresponse['Parameters'][0]['Value']

        if ARGS.VERBOSE > 7:
            print('BEARER: {}'.format(BEARER_TOKEN))

    while GET_DATA == "true":

        header_dict = {}

        header_dict['Accept'] = 'application/json'
        header_dict['Content-Type'] = 'application/json'
        header_dict['Authorization'] = 'Bearer ' + BEARER_TOKEN

        get_response = requests.post(DROPBOX_TARGET_URL,
                                     data=json.dumps(json_data),headers=header_dict)

        my_status = get_response.status_code
        my_payload = get_response.content

        if get_response.status_code != 200:
            logging.error('status_code: %d - %s', my_status, my_payload)
            sys.exit(get_response.status_code)
        else:
            dropbox_json_logs = json.loads(get_response.content)
        events = dropbox_json_logs['events']

        events_size = len(events)
        TOTAL_EVENTS += events_size

        if not os.path.isfile(DROPBOX_SUMS_FILE):
            starttext = 'Initialized:' + DROPBOX_SUMS_FILE
            START_STRING = hashlib.md5(starttext.encode('utf-8')).hexdigest()
            with open(DROPBOX_SUMS_FILE, 'a+') as output_sums:
                output_sums.write('{}\n'.format(START_STRING))

        SUM_LIST = [line.rstrip('\n') for line in open(DROPBOX_SUMS_FILE)]

        output_file = open(DROPBOX_LOGS_FILE, 'a+')
        output_sums = open(DROPBOX_SUMS_FILE, 'a+')
        for event in events:

            jsonstamp = event['timestamp'].replace('T', ' ').replace('Z', '')
            jsontime = datetime.datetime.strptime(jsonstamp, "%Y-%m-%d %H:%M:%S")

            utcdate = jsontime.replace(tzinfo=datetime.timezone.utc)
            original_timestamp = utcdate.strftime("%Y-%m-%d %H:%M:%S %Z")
            event['original_timestamp'] = original_timestamp

            curdate = utcdate.astimezone()
            adjusted_timestamp = curdate.strftime("%Y-%m-%d %H:%M:%S %Z")
            event['adjusted_timestamp'] = adjusted_timestamp

            json_log = json.dumps(event)
            json_log = json_log.rstrip('\n')
            JSON_SUM = hashlib.md5(json_log.encode('utf-8')).hexdigest()

            if JSON_SUM in SUM_LIST:
                if ARGS.VERBOSE > 7:
                    print('Skipping Event. Checksum: {}'.format(JSON_SUM))

            if JSON_SUM not in SUM_LIST:
                output_file.write('{}\n'.format(json_log))
                output_sums.write('{}\n'.format(JSON_SUM))
                if ARGS.VERBOSE > 7:
                    print(json.dumps(event, indent=4))

        if dropbox_json_logs['has_more'] == 'true':
            DROPBOX_TARGET_URL = 'https://api.dropboxapi.com/2/team_log/get_events/continue'
            json_data = { 'cursor': dropbox_json_logs['cursor'] }
            if ARGS.VERBOSE > 7:
                print('Retrieved: {} bytes and getting more data.'.format(events_size))
        else:
            GET_DATA = "false"
