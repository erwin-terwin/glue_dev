
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
# Initialize Glue client
glue_client = boto3.client('glue', region_name='us-east-1')
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
s3_client = boto3.client('s3')
#list the files in the location
bucket_name='athstat-etl-migrated'




option=''
if option=='prod':

    pg_config = {
        'dbname': 'athstat_games',
        'user': 'postgres',
        'password': 'J4VGzZwjfrcymkasdAsdkA',
        'host': 'athstat-analytics-prod-postgresql.cfmehnnvb5ym.us-east-1.rds.amazonaws.com',
        'port': 5432
    }

elif option=='qa':
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
game_ids=game_ids[0:1]
FAILED=[]
ACTIONS=[]
GAME_ACTIONS=[]

for game_id in game_ids:
    print('Processing : ',game_id)
    try:
        match_time_line_json_path=games_dictionary[game_id]['match_timeline']


        response = s3_client.get_object(Bucket=bucket_name, Key=match_time_line_json_path)
        json_string = response['Body'].read().decode('utf-8')
        json_data = json.loads(json_string)
        import pandas as pd

        # Assuming json_data is a list of dictionaries
        # Replace json_data with your actual data

        # Convert the list of dictionaries to a Pandas DataFrame
        df = pd.DataFrame(json_data)

        # Print or use the DataFrame as needed
        print(df)
        #sprt by timeOfDay ASC
        df.sort_values(by=['timeOfDay'], inplace=True, ascending=True)

        #convert to utc

        #sprt by matchTimeMs
        df.sort_values(by=['matchTimeMs'], inplace=True, ascending=True)

        #game_time_seconds

        df['game_time_seconds']=(df['matchTimeMs']-df['matchTimeMs'].min())/1000

        # Assuming df is your DataFrame with 'game_time_seconds' and 'timeOfDay' columns
        #start rea cloclk at o
        real_time=0
        import time
        data_source='mobii'

        current_time=datetime.now()
        elapsed_time=0
        for index, row in df.iterrows():
            print(row['game_time_seconds'], row['timeOfDay'])
            game_time_seconds=row['game_time_seconds']
            elapsed_time=game_time_seconds-real_time
            real_time=game_time_seconds
            delay_time=elapsed_time
            game_actions=row['options']
            try:
                keys = list(game_actions.keys())
                GAME_ACTIONS.extend([f'{key} {game_actions[key]}' for key in keys])


            except:
                pass
            homeTeamScore=row['homeTeamScore']
            awayTeamScore=row['awayTeamScore']
            team=row['team']
            team_id=team['teamId']
            athstat_team_id=generate_uuid(team_id,data_source)
            playerOne=row['playerOne']
            playerTwo=row['playerTwo']
            timestamp=row['timeOfDay']
            update_list=[]
            action_keys=game_actions.keys()
            action_name=row['name']
            if action_name=='Tackler Entry':
                outcome=[game_actions[key] for key in keys]
                outcome=outcome[0]
                action_name=action_name+outcome

            print(action_name)
            #remove whiute space
            action_name=action_name.replace(" ", "")
            ACTIONS.append(action_name)

            update_dict={
                        "game_id":generate_uuid(game_id,data_source),
                        "team_id":generate_uuid(team_id,data_source),
                        "timestamp":timestamp,
                        "game_time":game_time_seconds,
                        "action":action_name,
                        "action_count":1,
                        "team_score":homeTeamScore,
                        "opposition_score":awayTeamScore,
                    }

            update_list.append(update_dict)

            try:
                bulk_upsert_data(table='sports_action_pbp_live', data_dict=update_list, conflict_ids=['game_id','team_id','timestamp'],pg_config=pg_config)
            except Exception as e:
                print('Error',e)

            print('\n')

            print('sleeping for',delay_time)
            #time.sleep(delay_time)

    except:
            print('Failed ')
            FAILED.append(game_id)


ACTIONS=list(set(ACTIONS))
for i in ACTIONS:
    print(i)

print('\n')
print('Game actions')
GAME_ACTIONS=list(set(GAME_ACTIONS))
for i in GAME_ACTIONS:
    print(i)