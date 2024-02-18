
"""
Script reads the following tables and check for each field, null values and data quality and any duplicate values


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
import pandas as pd
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
        'dbname': 'athstat_analytics_qa',
        'user': 'postgres',
        'password': 'n4fn8s0Ffn4ssPx9Ujn4',
        'host': 'ec2-44-202-156-120.compute-1.amazonaws.com',
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



def get_primary_keys(pg_config, table_name):
    try:
        # Connect to the PostgreSQL database
        connection = psycopg2.connect(**pg_config)

        # Build the SQL query to retrieve Primary Key information
        query = sql.SQL("""
            SELECT a.attname as column_name
            FROM pg_index i
            JOIN pg_attribute a ON a.attnum = ANY(i.indkey)
            WHERE i.indrelid = '{}'::regclass AND i.indisprimary;
        """).format(
            sql.Identifier(table_name)
        )

        # Execute the query and fetch the result
        with connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()

        # Extract the column names from the result
        primary_keys = [row[0] for row in result]

        return primary_keys

    except Exception as e:
        print(f"Error: {e}")

    finally:
        # Close the connection
        if connection:
            connection.close()

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



def analyze_table(pg_config, table_name, data_source):
    # Read table into a DataFrame
    df = read_table_into_dataframe(pg_config, table_name)

    # Create a list to store DataFrame for each column
    df_checks = []

    # Loop through each column
    for col in df.columns:
        # Check null values
        null_count = df[col].isnull().sum()
        null_percentage = (null_count / len(df)) * 100

        # Get primary keys
        PK = get_primary_keys(pg_config, table_name)
        if col in PK:
            PK = 'PK'
        else:
            PK = None

        # Handle DatetimeTZDtype separately
        if isinstance(df[col].dtype, pd.DatetimeTZDtype):
            df[col] = df[col].dt.tz_localize(None)  # Convert to regular datetime
          
        # Create DataFrame for the column and append to df_checks
        df_check = pd.DataFrame({
            'Table': table_name,
            'Fieldname': col,
            'Data Source': data_source,
            'Datatype': df[col].dtype,
            'Key': PK,
            'Percentage Null values': null_percentage
        }, index=[0])
        df_checks.append(df_check)

    # Concatenate all DataFrames in df_checks into a single DataFrame
    df_check = pd.concat(df_checks, ignore_index=True)

    # Save the result to CSV
    df_check.to_csv('data_checks/' + table_name + '_' + data_source + '.csv', index=False)
    print('\n')
    return df_check


#write fuction to check for duplicates in a df for for each column
def check_pandas_df_duplicates(df):
    #check for duplicates in each column
    duplicate_columns=[]
    for col in df.columns:
        print(col, 'duplicates:', df[col].duplicated().sum())
        if df[col].duplicated().sum()>0:
            duplicate_columns.append(col)
    return duplicate_columns


print('Reading tables from RDS: ', pg_config['dbname'])

#for each table : Table	 Fieldname	Data Source	Datatype	Key Percentage Null values into pandas df

#1.  ----------------------- Check data source is mlr --------------------------------------------------
data_source='mobii'

#sports
table_name='sports'
df=read_table_into_dataframe(pg_config, table_name)
df_check_sports=analyze_table(pg_config, table_name, data_source)

#competitions
table_name='competitions'
df=read_table_into_dataframe(pg_config, table_name)
#filter data_source
df_check_competitions=analyze_table(pg_config, table_name, data_source)
duplicate_competitions=check_pandas_df_duplicates(df)


#seasons
table_name='seasons'
df=read_table_into_dataframe(pg_config, table_name)
#filter data_source
df_check_seasons=analyze_table(pg_config, table_name, data_source)
print('Checking for duplicates in seasons !')
duplicate_seasons=check_pandas_df_duplicates(df)

#leagues
table_name='leagues'
df=read_table_into_dataframe(pg_config, table_name)
