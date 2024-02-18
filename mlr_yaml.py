import boto3
import psycopg2
from psycopg2 import sql
import json
import yaml
import uuid
import pandas as pd
import io
from datetime import datetime, timedelta
# Initialize Glue client
glue_client = boto3.client('glue', region_name='us-east-1')
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
s3_client = boto3.client('s3')
#list the files in the location
bucket_name='athstat-etl-migrated'

#folder
folder='0-data-raw/api-collected/major_league_rugby/'

#list all subfolders in this location
paginator = s3_client.get_paginator('list_objects')
page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=folder)
subfolders=[]
for page in page_iterator:
    for key in page['Contents']:
        subfolders.append(key['Key'])
subfolders=[x for x in subfolders if x!=folder]
print('\n')
for f in subfolders:
    print(f)


games_dictionary = {}

# Create games dictionary
for match_string in subfolders:
    game_id = match_string.split('/')[-1].split('.')[0]
    year = match_string.split('/')[-2]

    # Check if game_id is not already in the dictionary, create it
    if game_id not in games_dictionary:
        games_dictionary[game_id] = {'year': year}

    # Update the dictionary based on the content of the string
    if 'results' in match_string:
        games_dictionary[game_id]['results'] = match_string
    if 'match_timeline' in match_string:
        games_dictionary[game_id]['match_timeline'] = match_string
    if 'match_report' in match_string:
        games_dictionary[game_id]['match_report'] = match_string
    if 'live_match_timeline' in match_string:
        games_dictionary[game_id]['live_match_timeline'] = match_string





#convert to yaml file
yaml_file = yaml.dump(games_dictionary, default_flow_style=False)
#save to S3 in bucket athstat-etl-migrated in folder data_maps as mlr_games.yaml
s3_client.put_object(Body=yaml_file, Bucket=bucket_name, Key='data_maps/mlr_games.yaml')

print('Done!')

        