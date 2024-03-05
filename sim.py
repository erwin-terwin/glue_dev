
"""
Athstat ETL S3 to Postgres RDS

This script extracts data from S3, transforms it, and loads it into a PostgreSQL RDS instance.

Writes to the following tables:
- sports
- competitions
- seasons
- leagues
- teams
- games_seasons
- team_actions
- athletes
- teams_athletes
- game_roster
- sports_action
- team_pbp_actions
- player_pbp_actions
- sports_action_pbp





"""




import boto3
import psycopg2
from psycopg2 import sql
import json
import yaml
import uuid
import pandas as pd
import io
from datetime import datetime, timedelta
from datetime import datetime, timezone
from datetime import datetime
import pytz
# Initialize Glue client
glue_client = boto3.client('glue', region_name='us-east-1')
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
s3_client = boto3.client('s3')
#list the files in the location
bucket_name='athstat-etl-migrated'



data_source='mobii'
option=''
if option=='prod':

    pg_config = {
        'dbname': 'athstat_games',
        'user': 'postgres',
        'password': 'J4VGzZwjfrcymkasdAsdkA',
        'host': 'athstat-analytics-prod-postgresql.cfmehnnvb5ym.us-east-1.rds.amazonaws.com',
        'port': 5432
    }

elif option=='review':
    pg_config = {
        'dbname': 'athstat_games_qa_review',
        'user': 'postgres',
        'password': 'n4fn8s0Ffn4ssPx9Ujn4',
        'host': 'athstat-analytics-qa-postgresql.cfmehnnvb5ym.us-east-1.rds.amazonaws.com',
        'port': 5432
    }


elif option=='athstat_games':
    pg_config = {
        'dbname': 'athstat_games',
        'user': 'postgres',
        'password': 'n4fn8s0Ffn4ssPx9Ujn4',
        'host': 'athstat-analytics-qa-postgresql.cfmehnnvb5ym.us-east-1.rds.amazonaws.com',
        'port': 5432
    }
else:
    pg_config = {
        'dbname': 'local_rds_prod',
        'user': 'postgres',
        'password': 'example',
        'host': 'localhost',
        'port': 5432
    }





print('Writing to RDS',pg_config['dbname'])
print('\n')





def execute_query(query:str,pg_config:dict, values:list=None,bulk: bool=False)->None:
    """
    Execute a SQL query using psycopg2 and commit the changes to the database.

    Args:
        query (str): The SQL query to be executed.
        values (list, optional): The values to be used in the query placeholders. Defaults to None.
        bulk (bool, optional): Indicates whether bulk insertion should be used. Defaults to False.

    Returns:
        None
    """

    if bulk:
        successful_inserts = 0  # Initialize a counter for successful inserts

        try:
            connection = psycopg2.connect(**pg_config)
            cursor = connection.cursor()
            if values:
                cursor.executemany(query, values)  # Use executemany for multiple rows
            else:
                cursor.execute(query)
            connection.commit()
            successful_inserts += cursor.rowcount  # Get the number of rows affected

        except Exception as e:
            print("Error executing query:", e)
        finally:
            cursor.close()
            connection.close()
        print(f"Successful inserts: {successful_inserts}")
        print(f"Total rows: {len(values) if values else 1}")
        print(f"Percent successful: {successful_inserts / len(values) if values else 1}")
    else:
        try:
            connection = psycopg2.connect(**pg_config)
            cursor = connection.cursor()
            if values:
                cursor.execute(query, values)
            else:
                cursor.execute(query)
            connection.commit()
        except Exception as e:
            print("Error executing query:", e)
        finally:
            cursor.close()
            connection.close()

def upsert_data(table: str, data_dict: dict, conflict_ids: list,pg_config:dict) -> bool:
    """
    Upsert data into a PostgreSQL table using the specified conflict resolution strategy.

    Args:
        table (str): The name of the PostgreSQL table to upsert data into.
        data_dict (dict): A dictionary containing the data to be upserted.
        conflict_ids (list): A list of column names that define the conflict resolution strategy.

    Returns:
        bool: True if the upsert was successful, False otherwise.
    Raises:
        Exception: If data_dict is empty or not a dictionary.

    Note:
        This function uses the ON CONFLICT ... DO UPDATE syntax in PostgreSQL for upserting data.
        The conflict resolution is determined by the specified conflict_ids.
    """
    # Throw error if data_dict is empty or not a dict
    if not isinstance(data_dict, dict) or len(data_dict) == 0:
        raise Exception("Data must be a dictionary")
    
    if not isinstance(conflict_ids, list):
        conflict_ids = [conflict_ids]

    placeholders = ', '.join(['%s'] * len(data_dict))
    columns = ', '.join(data_dict.keys())
    update_sql = ', '.join([f"{key} = EXCLUDED.{key}" for key in data_dict])

    sql_query = sql.SQL(f"INSERT INTO {table} ({columns}) VALUES ({placeholders}) ON CONFLICT ({', '.join(map(str, conflict_ids))}) DO UPDATE SET {update_sql}")

    
    values = list(data_dict.values())
 
    try:
   
        execute_query(query=sql_query,values= values,pg_config=pg_config)
        print(f"Upserted {table} successfully ID: {data_dict[conflict_ids[0]]}")

        return True  # Upsert was successful
    except Exception as e:
        print(f"Error upserting {table} ID: {data_dict[conflict_ids[0]]}")
        print(f"Error: {str(e)}")
        return False  # Upsert was not successful


def bulk_upsert_data(table: str, data_dict: list, conflict_ids: list,pg_config:dict) -> bool:
    """
    Upsert data into a PostgreSQL table using a bulk insert strategy.

    Args:
        table (str): The name of the PostgreSQL table to upsert data into.
        data_dict (list): A list of dictionaries containing data to be upserted.
        conflict_ids (list): A list of column names that define the conflict resolution strategy.

    Returns:
        bool: True if the bulk upsert was successful, False otherwise.
    Raises:
        Exception: If data_dict is empty or not a list.

    Note:
        This function uses the ON CONFLICT ... DO UPDATE syntax in PostgreSQL for bulk upserting data.
        The conflict resolution is determined by the specified conflict_ids.
    """
    # Throw error if data_dict is empty or not a list
    if not isinstance(data_dict, list) or len(data_dict) == 0:
        raise Exception("Data must be a list of dictionaries")
    
    if not isinstance(conflict_ids, list):
        conflict_ids = [conflict_ids]

    placeholders = ', '.join(['%s'] * len(data_dict[0]))
    columns = ', '.join(data_dict[0].keys())
    update_sql = ', '.join([f"{key} = EXCLUDED.{key}" for key in data_dict[0]])

    sql_query = sql.SQL(f"INSERT INTO {table} ({columns}) VALUES ({placeholders}) ON CONFLICT ({', '.join(map(str, conflict_ids))}) DO UPDATE SET {update_sql}")
    values = [list(data.values()) for data in data_dict]
    try:
        execute_query(query=sql_query, values=values, bulk=True,pg_config=pg_config)
        print(f"Upserted {table} successfully")
        print("*" * 100)
        print("")
        return True  # Bulk upsert was successful
    except Exception as e:
        print(f"Error upserting {table}")
        print(f"Error: {str(e)}")
        print("*" * 100)
        print("")
        return False  # Bulk upsert was not successful

def generate_uuid(value:str,data_source)->str:
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, str(value)+data_source))

def read_s3_file(bucket_name:str, file_name:str)->str:
    logger.info(f'Reading {file_name}')
    obj = s3_client.get_object(Bucket=bucket_name, Key=file_name)
    logger.info(f'File read')
    return obj['Body'].read().decode('utf-8')


def list_files_in_folder(bucket_name: str, folder_name: str, max_keys: int = 1000) -> list:
    """
    List all files in a folder on S3

    Args:
        bucket_name (str): The name of the S3 bucket.
        folder_name (str): The name of the folder.
        max_keys (int): Maximum number of keys to retrieve per request.

    Returns:
        list: A list of file names in the specified folder.
    """
    files = []
    continuation_token = None
    print('Getting files .... ')
    while True:
        params = {"Bucket": bucket_name, "Prefix": folder_name, "MaxKeys": max_keys}
        if continuation_token:
            params["ContinuationToken"] = continuation_token

        response = s3_client.list_objects_v2(**params)

        if "Contents" in response:
            for obj in response["Contents"]:
                files.append(obj["Key"])

        if not response.get("IsTruncated"):
            break

        continuation_token = response.get("NextContinuationToken")
        print('Number of files:',len(files))
        

    return files



games_path='0-data-raw/api-collected/major_league_rugby/live-test-2024'
list_of_live_events=list_files_in_folder(bucket_name, games_path)
events_dict={}
event_ids=[]
for event in list_of_live_events:
    #split by / take last
    game_id=event.split('/')[-1]
    timeline=game_id.split('2024')[1]
    #add 2024 back
    timeline='2024'+timeline
    #reove first
   
    #split by . take first
    timeline=timeline.split('.')[0]
    #convert to pythondatetime
   

    #split by 2024 get first
    game_id=game_id.split('2024')[0] 
    #remove last character
    game_id=game_id[:-1] 
    events_dict[timeline]={'timeline':timeline,
                         'match_timeline':event,
                         'game_id':game_id}
    event_ids.append(timeline)

#Load the yaml file with the games
games_dictionary = yaml.safe_load(read_s3_file(bucket_name=bucket_name, file_name='data_maps/mlr_games.yaml'))
#take first key



standard_action_names_path="data_maps/rugby_actions_mapping.csv"
standard_action_names_file=read_s3_file(bucket_name=bucket_name, file_name=standard_action_names_path)
standard_action_names_df=pd.read_csv(io.StringIO(standard_action_names_file),sep=',')
data_source_column='MLR'

standard_pbp_action_names=standard_action_names_df
#fileter if Type == pbp
standard_pbp_action_names=standard_pbp_action_names[standard_pbp_action_names['Type']=='pbp']
#select data source column and action column
standard_pbp_action_names=standard_pbp_action_names[[data_source_column,'Athstat action']]
#convert to dictionary
standard_pbp_action_names_dict=standard_pbp_action_names.set_index(data_source_column).to_dict()['Athstat action']

standard_action_names_df=standard_action_names_df[[data_source_column,'Athstat action']]
standard_action_names_dict=standard_action_names_df.set_index(data_source_column).to_dict()['Athstat action']



#pick random game in games dictionary
game_ids=list(games_dictionary.keys())
game_ids=game_ids[21:23]
g=game_ids[0]
game_ids=[g]
FAILED=[]
ACTIONS=[]
GAME_ACTIONS=[]

for event_key in event_ids:
    print('Processing : ',event_key)
    postgres_list=[]
    match_time_line_json_path=events_dict[event_key]['match_timeline']
    game_id=events_dict[event_key]['game_id']
    timeline=event_key

    # Define the correct format of the input string
    correct_format = '%Y-%m-%dT%H:%M:%S'

    # Parse the string into a datetime object
    dt_object = datetime.strptime(timeline, correct_format)

    # Print the result
    print(dt_object)

    
    response = s3_client.get_object(Bucket=bucket_name, Key=match_time_line_json_path)
    json_string = response['Body'].read().decode('utf-8')
    json_data = json.loads(json_string)
    teams=json_data.get('teams')

  
    for team in teams:
        teamId=team.get('teamId')
        team_name=team.get('name')
        imagePath=team.get('imagePath')
        isHomeTeam=team.get('isHomeTeam')
        score=team.get('score')
        metrics=team.get('metrics')
        for metric in metrics:
            name=metric.get('name')
            metricId=metric.get('metricId')
            value=metric.get('value')

            postgres_dict={
            "game_id":generate_uuid(game_id,data_source),
            "team_id":generate_uuid(teamId,data_source),
            "timestamp":timeline,
            "action":name,
            "action_total":value,
            "team_name":team_name,
            "metric_id":metricId
        }

        postgres_list.append(postgres_dict)
           
    #after updating list now upsert thje tble sports_action_pbp_live_cumulative on game_id,team_id and timestamp
    bulk_upsert_data(table='sports_action_live_pbp_cumulative', \
                     data_dict=postgres_list, conflict_ids=['game_id', 'team_id','timestamp'],pg_config=pg_config)

       

     






 