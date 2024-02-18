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

# list the files in the location
bucket_name = 'athstat-etl-migrated'

option = ''
if option == 'prod':
    pg_config = {
        'dbname': 'athstat_analytics_prod',
        'user': 'postgres',
        'password': 'J4VGzZwjfrcymkasdAsdkA',
        'host': 'ec2-54-87-75-32.compute-1.amazonaws.com',
        'port': 5432
    }
elif option == 'qa':
    pg_config = {
        'dbname': 'local_rds_prod',
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

import psycopg2
from psycopg2 import sql

def read_column_from_table(pg_config, table_name, column_name):
    try:
        # Connect to the PostgreSQL database
        connection = psycopg2.connect(**pg_config)

        # Create a cursor object to execute SQL queries
        cursor = connection.cursor()

        # Build the SQL query dynamically using the `sql` module
        query = sql.SQL("SELECT {} FROM {}").format(
            sql.Identifier(column_name),
            sql.Identifier(table_name)
        )

        # Execute the SQL query
        cursor.execute(query)

        # Fetch all rows from the result set
        rows = cursor.fetchall()

        return rows

    except Exception as e:
        print(f"Error: {e}")

    finally:
        # Close the cursor and connection
        if cursor:
            cursor.close()
        if connection:
            connection.close()

import pandas as pd

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


print('Writing to RDS', pg_config['dbname'])
print('\n')

#Get sports_action table intp dataframe
sports_action_df = read_table_into_dataframe(pg_config, 'sports_action_pbp')
#.1 get all data sources
# data_sources = sports_action_df['data_source'].unique().tolist()
# print('Data sources:', data_sources)
# for data_source in data_sources:
#     sports_action_source= sports_action_df[sports_action_df['data_source'] == data_source]
    


#select ation coumn
sports_action_df = sports_action_df[['action']]
#put in list python
actions = sports_action_df['action'].tolist()

#selection unique columns
actions = list(set(actions))
for action in actions:
    print(action)