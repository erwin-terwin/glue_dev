import boto3
import psycopg2
from psycopg2 import sql
import json
import yaml
import uuid
import pandas as pd
import io
import requests
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
        'dbname': 'athstat_games',
        'user': 'postgres',
        'password': 'n4fn8s0Ffn4ssPx9Ujn4',
        'host': 'athstat-analytics-qa-postgresql.cfmehnnvb5ym.us-east-1.rds.amazonaws.com',
        'port': 5432
    }


elif option=='review':
        pg_config = {
        'dbname': 'prod_review_delete',
        'user': 'postgres',
        'password': 'J4VGzZwjfrcymkasdAsdkA',
        'host': 'athstat-analytics-prod-postgresql.cfmehnnvb5ym.us-east-1.rds.amazonaws.com',
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

from psycopg2 import sql
from psycopg2.extras import execute_values
import psycopg2

def update_data(table: str, data_dict: dict, condition_field: str, pg_config: dict) -> bool:
    """
    Update data in a PostgreSQL table based on a specified condition.

    Args:
        table (str): The name of the PostgreSQL table to update.
        data_dict (dict): A dictionary containing the data to be updated.
        condition_field (str): The column used as a condition for the update.
        pg_config (dict): PostgreSQL configuration parameters.

    Returns:
        bool: True if the update was successful, False otherwise.
    Raises:
        Exception: If data_dict is empty or not a dictionary.

    Note:
        This function uses the UPDATE syntax in PostgreSQL for updating data.
    """
    # Throw error if data_dict is empty or not a dict
    if not isinstance(data_dict, dict) or len(data_dict) == 0:
        raise Exception("Data must be a dictionary")

    # Extract columns and values from the data_dict
    columns = ', '.join(data_dict.keys())
    update_sql = ', '.join([f"{key} = %s" for key in data_dict])

    # Construct the SQL query
    sql_query = sql.SQL(f"UPDATE {table} SET {update_sql} WHERE {condition_field} = %s")

    # Add the condition value to the values list
    values = list(data_dict.values()) + [data_dict.get(condition_field)]

    try:
        # Execute the update query
        execute_query(query=sql_query, values=values, pg_config=pg_config)
        print(f"Updated {table} successfully based on {condition_field} = {data_dict.get(condition_field)}")
        return True  # Update was successful
    except Exception as e:
        print(f"Error updating {table} based on {condition_field} = {data_dict.get(condition_field)}")
        print(f"Error: {str(e)}")
        return False  # Update was not successful


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


yaml_file_path='data_maps/urc_monitored_games.yaml'
yaml_file = read_s3_file(bucket_name, yaml_file_path)
games_file_dict = yaml.safe_load(yaml_file)

action_ontology_path="data_maps/rugby_actions_mapping.csv"
action_ontology_file = read_s3_file(bucket_name, action_ontology_path)
action_ontology_df = pd.read_csv(io.StringIO(action_ontology_file),sep=",")
data_source_column ="URC"

action_ontology_df=action_ontology_df[[data_source_column,'Athstat action']]
action_ontology_df=action_ontology_df.dropna(subset=[data_source_column])
action_ontology_dict=action_ontology_df.set_index(data_source_column).to_dict()["Athstat action"]

rugby_position_ontology_bytes= read_s3_file(bucket_name=bucket_name, file_name="data_maps/rugby_15s_position_ontology.csv")
rugby_position_ontology_bytes = io.StringIO(rugby_position_ontology_bytes)
rugby_position_ontology_df = pd.read_csv(rugby_position_ontology_bytes)
rugby_position_ontology_df=rugby_position_ontology_df.dropna(subset=["urc_position"])
position_class_dict=rugby_position_ontology_df.set_index("urc_position").to_dict()["athstat_position"]
import requests

teams_list=[]
for one_game in games_file_dict['games']:
    try:
        print("One game", one_game)
        list_of_games = one_game['endpoints']

        path_to_file = list_of_games['prefix']
        competition_id = list_of_games['competition_id']
        competition_name = list_of_games['competition_name']
        data_source = list_of_games['data_source']
        league_name = list_of_games['league_name']
        organization_id = list_of_games['organization_id']
        organization_name = list_of_games['organization_name']
        season_end_date = list_of_games['season_end'].replace("‘", "'").replace("’", "'")
        season_start_date = list_of_games['season_start'].replace("‘", "'").replace("’", "'")
        sport_id = list_of_games['sport_id']
        sport_name = list_of_games['sport_name']

        game_file = json.loads(read_s3_file(bucket_name, path_to_file)).get('data', {})

        athlete_data = []
        list_of_teams = ["homeTeam", "awayTeam"]

        for team_obj in list_of_teams:
                try:
                 image_url=game_file.get(team_obj, {}).get("imageUrl")
                except:
                    image_url=None
               

                try:
                     on_dark_image_url=game_file.get(team_obj, {}).get('imageUrls').get("ON_DARK")
                except: 
                    on_dark_image_url=None

                try:
                    on_light_image_url=game_file.get(team_obj, {}).get('imageUrls').get('DEFAULT')
                except:
                    on_light_image_url=None




                


                try:
                   
                    generated_id = generate_uuid(game_file.get(team_obj).get("id"),data_source=data_source)
                    #download tea image_iurl to local if not None
                    if image_url != None:
                        team_image_path = f'local_images/{generated_id}.png'
                        team_image_data = requests.get(image_url).content
                        with open(team_image_path, 'wb') as handler:
                            handler.write(team_image_data)
                        #S3_image_path
                        S3_image_path = f'logos/{generated_id}.png'
                        s3_client.upload_file(team_image_path, 'athstat-landing-assets-migrated', S3_image_path)
                        #get S3 url
                        image_url = f'https://athstat-landing-assets-migrated.s3.amazonaws.com/{S3_image_path}'
                    else:
                        image_url = None
                    

                    #on dark
                    if on_dark_image_url != None:
                        team_image_path = f'local_images/{generated_id}_on_dark.png'
                        team_image_data = requests.get(on_dark_image_url).content
                        with open(team_image_path, 'wb') as handler:
                            handler.write(team_image_data)
                        #S3_image_path
                        S3_image_path = f'logos/{generated_id}_on_dark.png'
                        s3_client.upload_file(team_image_path, 'athstat-landing-assets-migrated', S3_image_path)
                        #get S3 url
                        on_dark_image_url = f'https://athstat-landing-assets-migrated.s3.amazonaws.com/{S3_image_path}'
                    else:
                        on_dark_image_url = None

                    #on light
                    if on_light_image_url != None:
                        team_image_path = f'local_images/{generated_id}_on_light.png'
                        team_image_data = requests.get(on_light_image_url).content
                        with open(team_image_path, 'wb') as handler:
                            handler.write(team_image_data)
                        #S3_image_path
                        S3_image_path = f'logos/{generated_id}_on_light.png'
                        s3_client.upload_file(team_image_path, 'athstat-landing-assets-migrated', S3_image_path)
                        #get S3 url
                        on_light_image_url = f'https://athstat-landing-assets-migrated.s3.amazonaws.com/{S3_image_path}'
                    else:
                        on_light_image_url = None

                 
                    team_dict = {
                        "athstat_id": generated_id,
                        'image_url': image_url,
                        'on_dark_image_url': on_dark_image_url,
                        'on_light_image_url': on_light_image_url
                       
                    }
                    teams_list.append(team_dict)

                except Exception as e:
                    print(f"Error processing player: {str(e)}")
                    pass

        try:
            #update
            update_data(table='teams', data_dict=team_dict, condition_field='athstat_id', pg_config=pg_config)
            print('-------------------------------------------------------------------------\n')
            print(team_dict)
            print('\n')
            print('Inserted Images to Postgres: teams in', pg_config['dbname'])
            print('\n')
        except Exception as e:
            print(f"Error upserting data into PostgreSQL: {str(e)}")
    except Exception as e:
        print(e)