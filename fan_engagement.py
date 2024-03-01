"""

Athstat DATA-AI 2024

Script  Makes queries from RDS Postgres SQL and Writes  to Opensearch.
The script writes fan engagement data to an Opensearch Index

** Context **
As part of the API call, you will receive multiple data points which are defined in the data dictionary found here. 
However there are specific data points we would like to single out that are meant for fan engagement.
Namely, betting odds, head to head history and last 5 game records for each team.


"""







# Import Modules and Libraries
import boto3
import psycopg2
from psycopg2 import sql
import json
import yaml
import uuid
import pandas as pd
import io
import requests

import pandas as pd

from requests.auth import HTTPBasicAuth

from opensearchpy import OpenSearch



# Initialize Glue client
glue_client = boto3.client('glue', region_name='us-east-1')
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
s3_client = boto3.client('s3')
#list the files in the location
bucket_name='athstat-etl-migrated'




#Configuration

option='qa'
opensearch_option='qa'


if opensearch_option=='qa':
        url = 'https://qa-opensearch.athstat-next.com'
        port = 9200
        username = 'admin'
        password = 'ASdkmVlkasdAsdkASDwe'
else:
    pass


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




print(f'Reading from Postgres SQL: {pg_config["dbname"]} {option}')
print('\n')





# Functions


def read_table_query(query: str, pg_config: dict, fetch: bool = False) -> pd.DataFrame:
    """
    Execute a SQL query using psycopg2 and optionally fetch the results.

    Args:
        query (str): The SQL query to be executed.
        pg_config (dict): PostgreSQL configuration parameters.
        fetch (bool, optional): Indicates whether to fetch the results. Defaults to False.

    Returns:
        pd.DataFrame or None: If fetch is True, returns a pandas DataFrame with the query results.
                              Otherwise, returns None.
    """

    result_df = None  # Initialize result DataFrame

    try:
        with psycopg2.connect(**pg_config) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query)

                if fetch:
                    result_df = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])

            connection.commit()

    except Exception as e:
        print("Error executing query:", e)

    return result_df



def write_to_opensearch(df_pandas, index_name, host,upsert_id, port, username, password):
        


        #writing to OpenSeach
        #save to Opensearch
        # Initialize the OpenSearch client (your configuration)
        host = 'qa-opensearch.athstat-next.com'  # Use the URL from your configuration
        port = 9200
        auth = ('admin', 'ASdkmVlkasdAsdkASDwe')  # Replace with your credentials
        client = OpenSearch(
            hosts=[{'host': host, 'port': port}],
            http_compress=True,
            http_auth=auth,
            use_ssl=True,
            verify_certs=False,
            ssl_assert_hostname=False,
            ssl_show_warn=False
        )

        id_field = upsert_id
        #repalce white space in index name with -
        index_name=index_name.replace(' ','-')
        #lower case index name
        index_name=index_name.lower()

        # Print a message indicating that you're writing to OpenSearch
        print('Writing to OpenSearch!')

        # Write the DataFrame to OpenSearch using the index method
        for _, doc in df_pandas.iterrows(): 
        
            client.index(index=index_name, id=doc[id_field], body=doc.to_dict())

        # Print a message indicating that the write operation is complete
        print('Written to OpenSearch!')

        # To read the data from OpenSearch, you can use the search method
        response = client.search(index=index_name)
        print(response)
        return response




#-------------------------------------- MAIN -----------------------------------------------


#1. Read data from table games_seasons
games_seasons = read_table_query("SELECT * FROM games_seasons", pg_config, fetch=True)
#2. Read data from table game_odds
game_odds = read_table_query("SELECT * FROM game_odds", pg_config, fetch=True)
#3. Read data from table teams
teams = read_table_query("SELECT * FROM teams", pg_config, fetch=True)
#4. Read data from table leagues
leagues = read_table_query("SELECT * FROM leagues", pg_config, fetch=True)
#5. Read data from table sports
sports = read_table_query("SELECT * FROM sports", pg_config, fetch=True)
#6. Read data from sports_action_pbp_live
sports_action_pbp_live = read_table_query("SELECT * FROM sports_action_pbp_live", pg_config, fetch=True)

games_dfs=[]
for index, row in game_odds.iterrows():
    game_id=row['game_id']
    #query from games_seasons based on matching game_id the following fields:
    # team_id, opposition_team_id,venue,kickoff_time,round and status
    team_id=games_seasons.loc[games_seasons['game_id'] == game_id, 'team_id'].values[0]
    opposition_team_id=games_seasons.loc[games_seasons['game_id'] == game_id, 'opposition_team_id'].values[0]
    venue=games_seasons.loc[games_seasons['game_id'] == game_id, 'venue'].values[0]
    kickoff_time=games_seasons.loc[games_seasons['game_id'] == game_id, 'kickoff_time'].values[0]
    game_round=games_seasons.loc[games_seasons['game_id'] == game_id, 'round'].values[0]
    #game_status=games_seasons.loc[games_seasons['game_id'] == game_id, 'game_status'].values[0]
    game_status='not_started'
    #from teams based team_id and opposition_team_id query
    home_name=teams.loc[teams['athstat_id'] == team_id, 'athstat_name'].values[0]
    away_name=teams.loc[teams['athstat_id'] == opposition_team_id, 'athstat_name'].values[0]
    shortname_home=teams.loc[teams['athstat_id'] == team_id, 'athstat_abbreviation'].values[0]
    shortname_away=teams.loc[teams['athstat_id'] == opposition_team_id, 'athstat_abbreviation'].values[0]
    sport_id=teams.loc[teams['athstat_id'] == team_id, 'sport_id'].values[0]
    sport=sports.loc[sports['id'] == sport_id, 'name'].values[0]
    home_prob=row['home_prob']
    away_prob=row['away_prob']
    draw_prob=row['draw_prob']
    home_odds=row['home_odd']
    away_odds=row['away_odd']
    draw_odds=row['draw_odd']
    league_id=games_seasons.loc[games_seasons['game_id'] == game_id, 'league_id'].values[0]
    league_name=leagues.loc[leagues['id'] == league_id, 'name'].values[0]

    #head to head between teams
    head_to_head_games=games_seasons[(games_seasons['team_id'] == team_id) & (games_seasons['opposition_team_id']\
     == opposition_team_id)]
    # get team_id number of wins based on team_id and team_score and opposition_score
    home_wins=len(head_to_head_games[(head_to_head_games['team_score'] > head_to_head_games['opposition_score'])])
    # get opposition_team_id number of wins based on team_id and team_score and opposition_score
    away_wins=len(head_to_head_games[(head_to_head_games['team_score'] < head_to_head_games['opposition_score'])])
    # get number of draws based on team_id and team_score and opposition_score
    draws=len(head_to_head_games[(head_to_head_games['team_score'] == head_to_head_games['opposition_score'])])

    #get FORM last 5 WINS SEQUENCE for each team Left to Right for home_team
    last_5_games_home=games_seasons[(games_seasons['team_id'] == team_id) | (games_seasons['opposition_team_id']\
     == team_id)]
    last_5_games_home=last_5_games_home.sort_values(by='kickoff_time', ascending=False)
    last_5_games_home=last_5_games_home.head(5)
    last_5_games_home['result'] = last_5_games_home.apply(lambda x: 'W' if x['team_score'] > x['opposition_score'] \
    else ('L' if x['team_score'] < x['opposition_score'] else 'D'), axis=1)
    last_5_games_home=last_5_games_home['result'].values
    #convert to string left to right
    last_5_games_home=''.join(last_5_games_home)

    #get FORM last 5 WINS SEQUENCE for each team Left to Right for away_team
    last_5_games_away=games_seasons[(games_seasons['team_id'] == opposition_team_id) | (games_seasons['opposition_team_id']\
     == opposition_team_id)]
    last_5_games_away=last_5_games_away.sort_values(by='kickoff_time', ascending=False)
    last_5_games_away=last_5_games_away.head(5)
    last_5_games_away['result'] = last_5_games_away.apply(lambda x: 'W' if x['team_score'] > x['opposition_score'] \
    else ('L' if x['team_score'] < x['opposition_score'] else 'D'), axis=1)
    last_5_games_away=last_5_games_away['result'].values
    #convert to string left to right
    last_5_games_away=''.join(last_5_games_away)
    #home_team_image_ulr
    home_team_image_ulr=teams.loc[teams['athstat_id'] == team_id, 'image_url'].values[0]
    #away_team_image_ulr
    away_team_image_ulr=teams.loc[teams['athstat_id'] == opposition_team_id, 'image_url'].values[0]

    #put into json_dict
    game_dict={
        "event_id":game_id,
        "home_participant_id":team_id,
        "away_participant_id":opposition_team_id,
        "shortname_away":shortname_away,
        "shortname_home":shortname_home,
        "home_name":home_name,
        "away_name":away_name,
        "venue":venue,
        "start_time":kickoff_time,
        "stage":game_round,
        "status":game_status,
        "sport":sport,
        "sport_id":sport_id,
        "league_name":league_name,
        "league_id":league_id,
        "home_prob":home_prob,
        "away_prob":away_prob,
        "draw_prob":draw_prob,
        "home_odds":home_odds,
        "away_odds":away_odds,
        "draw_odds":draw_odds,
        "home_wins":home_wins,
        "away_wins":away_wins,
        "draws":draws,
        "last_5_games_home":last_5_games_home,
        "last_5_games_away":last_5_games_away  ,
        "home_team_image_ulr":home_team_image_ulr,
        "away_team_image_ulr":away_team_image_ulr
    }

    #convert to pandas dataframe
    game_df=pd.DataFrame([game_dict])
    games_dfs.append(game_df)









#concatenate all dataframes
games_df=pd.concat(games_dfs)

#write to opensearch index rugby-fan-engagement-test
write_to_opensearch(games_df, 'rugby-fan-engagement-test', url, 'event_id', port, username, password)

print('Data written to Opensearch Index: rubgy-fan-engagement-test')
    







