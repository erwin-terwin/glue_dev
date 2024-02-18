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

target_bucket='athstat-landing-assets-migrated'
target_folder='athstat-games'

option=''
if option=='prod':
    pg_config = {
    'dbname': 'athstat_analytics_prod',
    'user': 'postgres',
    'password': 'J4VGzZwjfrcymkasdAsdkA',
    'host': 'ec2-54-87-75-32.compute-1.amazonaws.com',
    'port': 5432
}
elif option=='qa':
    pg_config = {
        'dbname': 'athstat_analytics_qa',
        'user': 'postgres',
        'password': 'n4fn8s0Ffn4ssPx9Ujn4',
        'host': 'ec2-44-202-156-120.compute-1.amazonaws.com',
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


import psycopg2
from psycopg2 import sql

def update_table_with_images(pg_config, table_name, reference_key_column, field_to_update, images_list):
    # Connect to the PostgreSQL database using the provided configuration
    conn = psycopg2.connect(**pg_config)
    
    # Create a cursor object to execute SQL queries
    cur = conn.cursor()

    # Iterate through the input images_list and insert image_url into the specified table
    for entry in images_list:
        reference_key_value = entry.get(reference_key_column)
        field_value = entry.get(field_to_update)

        # Skip entries without reference_key_value or field_value
        if not reference_key_value or not field_value:
            continue

        # Use the reference_key_value to update the specified table
        query = sql.SQL("""
            UPDATE {}
            SET {} = %s
            WHERE {} = %s
        """).format(
            sql.Identifier(table_name),
            sql.Identifier(field_to_update),
            sql.Identifier(reference_key_column)
        )

        # Execute the query with the provided parameters
        cur.execute(query, (field_value, reference_key_value))

    # Commit the changes
    conn.commit()

    # Close the cursor and connection
    cur.close()
    conn.close()





yaml_file_path='data_maps/urc_monitored_games.yaml'
yaml_file = read_s3_file(bucket_name, yaml_file_path)
games_file_dict = yaml.safe_load(yaml_file)

position_ontology_path="data_maps/rugby_actions_mapping.csv"
position_ontology_file = read_s3_file(bucket_name, position_ontology_path)
position_ontology_df = pd.read_csv(io.StringIO(position_ontology_file), sep=',')
data_source_column ="URC"

position_ontology_df=position_ontology_df[[data_source_column,'Athstat action']]
position_ontology_df=position_ontology_df.dropna(subset=[data_source_column])
position_ontology_dict=position_ontology_df.set_index(data_source_column).to_dict()["Athstat action"]




# List objects in the specified folder
response = s3_client.list_objects_v2(
    Bucket='athstat-etl-migrated',
    Prefix='0-data-raw/api-collected/urc/game_reports'
)

# Extract file names from the response
list_of_files = [obj['Key'] for obj in response.get('Contents', [])]

# Print the list of files
for file_name in list_of_files:
    print(file_name)




images_list=[]
data_source='incrowed'
for path_to_file in list_of_files:
        try:
            game_file = json.loads(read_s3_file(bucket_name, path_to_file) ).get('data',{})
        except:
            continue




        player_stats = []
        athlete_data=[]
        team_athletes=[]
        roster=[]
        list_of_teams=["homeTeam","awayTeam"]
     
        for team_obj in list_of_teams:
            imageUrl = game_file[team_obj]['imageUrl']
            imageUrls=game_file[team_obj]['imageUrls']
            try:
                on_dark_image_url=imageUrls['ON_DARK']
            except:
                on_dark_image_url=None

            try:
                default_image_url=imageUrls['DEFAULT']
            except:
                default_image_url=None
        

            team_id=game_file[team_obj]['id']
            athstat_team_id=generate_uuid(team_id,data_source)

            #get team_image_url and save url to S3
            #dowload imageUrl to S3

            team_image_url = imageUrl
            #downlaod iamge to local athsat_games_images/teams
            team_image_name = f'{athstat_team_id}.png'
            team_image_path = f'logos/{team_image_name}'
            
            team_image_data = requests.get(team_image_url).content

            s3_client.put_object(Body=team_image_data, Bucket='athstat-landing-assets-migrated', Key=team_image_path)
            #get iamge publiuc url
            S3_url = f'https://athstat-landing-assets-migrated.s3.amazonaws.com/{team_image_path}'
            images_dict={
                'athstat_id':athstat_team_id,
                'image_url':S3_url
            }
            print('team_image_url',S3_url)
            print('\n')

            images_list.append(images_dict)

         


        



print('\n')
print('Updates complete!') 

table_name = 'teams'
reference_key_column = 'athstat_id'
field_to_update = 'image_url'

update_table_with_images(pg_config, table_name, reference_key_column, field_to_update, images_list)
print('Images updated successfully!')


