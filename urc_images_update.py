
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



def read_table_into_dataframe(pg_config, table_name):
    try:
        # Connect to the PostgreSQL database
        connection = psycopg2.connect(**pg_config)

        # Build the SQL query dynamically using the `sql` module
        query = sql.SQL("SELECT * FROM {}").format(
            sql.Identifier(table_name)
        )

        # Use Pandas to read the query result into a DataFrame
        df = pd.read_sql_query(query, connection)

        return df

    except Exception as e:
        print(f"Error: {e}")

    finally:
        # Close the connection
        if connection:
            connection.close()


def upsert_images(data_dict: dict, pg_config: dict) -> bool:
    if not isinstance(data_dict, dict) or not data_dict:
        raise Exception("Data must be a non-empty dictionary")

    table = "your_images_table"  # Replace with your actual table name
    columns = ', '.join(data_dict.keys())
    placeholders = ', '.join(['%s'] * len(data_dict))
    update_sql = ', '.join([f"{key} = EXCLUDED.{key}" for key in data_dict])

    # Assuming 'tracking_id' is the key for upserting
    sql_query = sql.SQL(
        f"INSERT INTO {table} ({columns}) VALUES ({placeholders}) "
        f"ON CONFLICT (tracking_id) "
        f"DO UPDATE SET {update_sql}"
    )

    values = list(data_dict.values())

    try:
        execute_query(query=sql_query, values=values, pg_config=pg_config)
        print(f"Upserted {table} successfully ID: {data_dict.get('tracking_id')}")
        return True  # Upsert was successful
    except Exception as e:
        print(f"Error upserting {table} ID: {data_dict.get('tracking_id')}")
        print(f"Error: {str(e)}")
        return False  # Upsert was not successful


print('Writing to RDS',pg_config['dbname'])
print('\n')


#read athleste table
df=read_table_into_dataframe(pg_config,'athletes')
df_images = df
df_images = df
#filter for data_source=incrowed
df_images=df_images[df_images['data_source']=='incrowed']
data_sources=df_images['data_source'].unique().tolist()
print('Data Sources:',data_sources)
print('\n')
images_list=[]
counter=0
number_of_rows=len(df_images)
for index, row in df_images.iterrows():
    source_id=row['source_id']
    tracking_id=row['tracking_id']
    image_url=f'https://media-cdn.incrowdsports.com/{source_id}.png'
    player_image_data = requests.get(image_url).content
    player_image_path = f'logos/{tracking_id}.png'
    s3_client.put_object(Body=player_image_data, Bucket='athstat-landing-assets-migrated', Key=player_image_path)
    S3_url = f'https://athstat-landing-assets-migrated.s3.amazonaws.com/logos/{tracking_id}.png'
    print('Image uploaded to S3 !: Precentage Complete:',(counter/number_of_rows)*100)
    
    athlete_dict={
                "source_id":row['source_id'],
                "tracking_id":row['tracking_id'],
                "player_name":row['player_name'],
                "nick_name":row['nick_name'],
                # "birth_country":None,
                # "date_of_birth":None,
                # "abbr":None,
                "athstat_name":row['athstat_name'],
                "athstat_firstname":row['athstat_firstname'],
                "athstat_lastname":row['athstat_lastname'],
              
                "team_id":row['team_id'], 
                # "age":None,
                # "height":None,
                # "weight":None,           
              
                "data_source":row['data_source'],
                "position":row['position'],#map thiscc
                'image_url':S3_url
                # "positionName":player.get('positionName'),#map this
    }
    images_list.append(athlete_dict)
    counter+=1




#inset images
bulk_upsert_data(table='athletes',data_dict= images_list, conflict_ids='tracking_id',pg_config=pg_config)
print('Images inserted into RDS successfully!')