#pylint: disable=all

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
bucket_name='athstat-etl-migrated'



option=''
if option=='prod':
    pg_config = {
            'dbname': 'prod_review_delete',
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

        try:
            # Close the connection
            if connection:
                connection.close()
        except:
            pass


###########################################################


def joining_tables() -> pd.DataFrame:

    """
    Joins tables 'sports_action_pbp', 'teams', and 'games_seasons' from the database specified in `pg_config`.
    Returns a pandas DataFrame with merged data.
    
    Returns:
    -------
    pandas.DataFrame
        A DataFrame containing merged data from the specified tables.
        
    Notes:
    ------
    This function performs the following steps:
    1. Reads the 'sports_action_pbp', 'teams', and 'games_seasons' tables into separate DataFrames.
    2. Filters the 'teams' DataFrame to include only rows where the 'data_source' column is 'mlr'.
    3. Renames columns in the 'sports_action_pbp' DataFrame to 'team_score_pbp', 'opposition_score_pbp', and 'team_id_pbp'.
    4. Merges the 'sports_action_pbp' and 'games_seasons' DataFrames on the 'game_id' column.
    5. Assigns team and opponent names to the DataFrame based on the 'mlr_teams_ids' dictionary.
    6. Returns the resulting DataFrame.
    """

    table_name = "sports_action_pbp"
    df_mlr = read_table_into_dataframe(pg_config, table_name)

    table_name = "teams"
    df_teams = read_table_into_dataframe(pg_config, table_name)

    table_name = "games_seasons"
    df_scores = read_table_into_dataframe(pg_config, table_name)

 
    df_teams_mlr = df_teams[df_teams['data_source'] == 'mlr']


    #rename the team_score and opposition_score columns to team_score_pbp and opposition_score_pbp
    df_mlr = df_mlr.rename(columns = {'team_score':'team_score_pbp','opposition_score':'opposition_score_pbp','team_id':'team_id_pbp'})


    #merge df and df_scores in game_id
    df = pd.merge(df_mlr,df_scores,how='left',left_on='game_id',right_on='game_id')

    mlr_teams_ids = {}

    for index, row in df_teams_mlr.iterrows():
        mlr_teams_ids[row['athstat_name']] = row['athstat_id']

    df['team_name'] = 0
    df['opponent_name'] = 0

    for index,row in df.iterrows():
        if  row['opposition_team_id'] in mlr_teams_ids.values():
            for key,value in mlr_teams_ids.items():
                if value == row['opposition_team_id']:
                    df.at[index,'opponent_name'] = key
        
        if row['team_id'] in mlr_teams_ids.values():
            for key_team,value_team in mlr_teams_ids.items():
                if value_team == row['team_id']:
                    df.at[index,'team_name'] = key_team   


    for index,row in df.iterrows():
        if row['team_id_pbp'] in mlr_teams_ids.values():
            for key_team,value_team in mlr_teams_ids.items():
                if value_team == row['team_id_pbp']:
                    df.at[index,'team'] = key_team

    
    return df


#######################################################################################################################

def read_pre_game_odds(table_name:str) -> pd.DataFrame:

    """
    Reads the 'pre_game_odds' table from the database specified in `pg_config` into a pandas DataFrame.
    Returns a pandas DataFrame with the data from the 'game_odds' table.
    
    Returns:
    -------
    pandas.DataFrame
        A DataFrame containing the data from the 'game_odds' table.
        
    Notes:
    ------
    This function performs the following steps:
    1. Reads the 'game_odds' table into a pandas DataFrame.
    2. Returns the resulting DataFrame.
    """

    df_pre_game_odds = read_table_into_dataframe(pg_config, table_name)

    return df_pre_game_odds



def read_sports_action_pbp_live(table_name:str) -> pd.DataFrame:

    """
    Reads the 'sports_action_pbp_live' table from the database specified in `pg_config` into a pandas DataFrame.
    Returns a pandas DataFrame with the data from the 'sports_action_pbp_live' table.
    
    Returns:
    -------
    pandas.DataFrame
        A DataFrame containing the data from the 'sports_action_pbp_live' table.
        
    Notes:
    ------
    This function performs the following steps:
    1. Reads the 'sports_action_pbp_live' table into a pandas DataFrame.
    2. Returns the resulting DataFrame.
    """

    df_sports_action_pbp_live = read_table_into_dataframe(pg_config, table_name)

    return df_sports_action_pbp_live