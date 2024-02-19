import boto3
import psycopg2
from psycopg2 import sql
import json
import yaml
import uuid
import pandas as pd
import io
from datetime import datetime, timedelta
from reading_postgres_data import joining_tables, read_pre_game_odds
from teams_xp_calculation_live import teams_xp_calculation
from ingame_predictions_calculation_live import in_game_predictions
from write_data_postgresql import write_live_odds
import time
# from reading_postgres_data import read_sports_action_pbp_live
pg_config = {
        'dbname': 'local_rds_prod',
        'user': 'postgres',
        'password': 'example',
        'host': 'localhost',
        'port': 5432
    }



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







while True:
    # Call the function to download data
    #df = joining_tables()
    df_postgres=read_sports_action_pbp_live('sports_action_pbp_live')
    #df_postgres = pd.read_csv('mlr_pbp_postgresql_v2.csv')
    game_id = 'b1ffed8b-f6d3-5ad6-8499-66023cff4ec0'
    print("Game ID: ", game_id)
    print(df_postgres.head())
    #get ubique team_ids from the dataframe
    team_ids = df_postgres['team_id'].unique()
    print(team_ids)
    # df_postgres= df_postgres[df_postgres['game_id'] == game_id]
    previous_length = 0
    # Check if the length of df has increased
    if len(df_postgres) > previous_length:
        # Get the last row
        df = pd.DataFrame(df_postgres.iloc[-1]).T
        df['home_team_xP'] = 0
        df['away_team_xP'] = 0
        df['score_diff'] = 0
        df['xp_diff'] = 0
        df['xp_diff_cum'] = 0
        #reset the index
        df = df.reset_index(drop = True)
        df_new = pd.DataFrame()  # Initialize an empty DataFrame to hold the sliced rows
        for index, row in df.iterrows():
            if index < 20:
                # Append the row to df_new
                df_xp = teams_xp_calculation(row)
                df_new = df_new.append(df_xp, ignore_index=True)
                df_new['xp_diff_cum'] = df_new['xp_diff'].cumsum()
                df_odds = in_game_predictions(df_new)
                # Write the calculated odds to the database
                # df_odds = pd.DataFrame(df_odds_all.iloc[-1]).T
                odds_list = []
                # for index,row in df_odds.iterrows():
                #     odds_list.append({'game_id':row['game_id'],'game_time':row['game_time'],'timestamp':row['timestamp'],'team_name':row['team_name'],'opponent_name':row['opponent_name'],'home_prob':row['home_prob'],'away_prob':row['away_prob']})
                # write_live_odds(odds_list,'live_odds')
        previous_length = len(df)
    # Wait for 30 seconds before the next iteration\
    print('Wating for udpate !')
    