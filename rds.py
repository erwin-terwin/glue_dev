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

option='qa'
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
        'dbname': 'athstat_games_qa_review',
        'user': 'postgres',
        'password': 'J4VGzZwjfrcymkasdAsdkA',
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

print('Reading from RDS', pg_config['dbname'])
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

# #read table athletes
# athletes = read_table_into_dataframe(pg_config, 'athletes')
# original_athletes = athletes.copy()
# #drop if image_url=None or empty
# athletes = athletes.dropna(subset=['image_url'])
# #filter to data_source=incrowed
# athletes = athletes[athletes['data_source']=='incrowed']
# #keep only unoque athletes_id
# athletes = athletes.drop_duplicates(subset='tracking_id')
# #for each unique data_source pring number of athletes with images
# print(athletes.groupby('data_source').size())
# print('athletes with images:', len(athletes))
# #from original athletes table print number of athletes with images for data_source as a percentage
# print('athletes with images:', len(athletes)/len(original_athletes))
# # #filter for data_source= incrowed

# #read athletes_price_history
# athlete_price_history = read_table_into_dataframe(pg_config, 'athlete_price_history')

# #games_seasons
# games_seasons = read_table_into_dataframe(pg_config, 'games_seasons')
# #sort by kickoff_time ascending
# games_seasons = games_seasons.sort_values(by='kickoff_time')

# #teams
# teams = read_table_into_dataframe(pg_config, 'teams')

# #leagues
# leagues = read_table_into_dataframe(pg_config, 'leagues')
# name='United Rugby Championship 22/23'
# #filter leagues by name
# league = leagues[leagues['name']==name]
# season_id=league['season_id'].values[0]
# #filter games_seasons if kickoff_times containts 2023
# # Ensure 'kickoff_time' column is in datetime format
# games_seasons['kickoff_time'] = pd.to_datetime(games_seasons['kickoff_time'])

# # Filter rows based on the year 2023
# games_seasons_2023 = games_seasons[games_seasons['kickoff_time'].dt.year == 2023]

# # Display the filtered DataFrame
# print(games_seasons_2023)

# #athletes
# athletes = read_table_into_dataframe(pg_config, 'athletes')
# #get all unique teams  via athstat_id and make dict athstat_id and athstat_name
# teams_dict = teams[['athstat_id','athstat_name']].drop_duplicates().set_index('athstat_id').to_dict()['athstat_name']
# team_keys_list = list(teams_dict.keys())


# # team_id='1fa00c37-f405-54f6-8bb5-78d3eac2f9c5'
# team_id='2d1304fe-c44d-5b98-b7a7-2aab6c398e00'
# teams_data_list=[]
# for team_id in team_keys_list:
#     # Filter rows based on the year 2023
#     games_seasons_2023 = games_seasons[games_seasons['kickoff_time'].dt.year == 2023]
#     #get all athletes with team_id into list
#     team_athletes=athletes[athletes['team_id']==team_id]
#     #filer games_seasons if team_id or opposition_id is team_id 2023
#     games_seasons_2023 = games_seasons_2023[(games_seasons_2023['team_id']==team_id) | (games_seasons_2023['opposition_team_id']==team_id)]

#     team_name=teams_dict[team_id]

#     games_ids_2023=games_seasons_2023['game_id'].values
#     #unique
#     games_ids_2023 = list(set(games_ids_2023))

#     #remove athlete_price_history if game_id not in games_ids_2023
#     athlete_price_history_2023=athlete_price_history[athlete_price_history['game_id'].isin(games_ids_2023)]
#     #remove if athlete_id not in team_athletes
#     athlete_price_history_2023=athlete_price_history_2023[athlete_price_history_2023['athlete_id'].isin(team_athletes['tracking_id'])]


#     #make dicte t save each athlets price in a list
#     athletes_price_dict = {}
#     team_value=[]
#     games=athlete_price_history_2023['game_id'].unique().tolist()

#     price_dict={}
#     count=1
#     for game in games:
#         game_data=athlete_price_history_2023[athlete_price_history_2023['game_id']==game]
#         #get price col and sort by desceding order and keep first 23
#         game_data=game_data.sort_values(by='updated_price',ascending=False)
#         game_data=game_data.head(23)
#         #get sum of price
#         team_value.append(game_data['updated_price'].sum())
#         price_dict[count]=game_data['updated_price'].sum()
#         count+=1



#     #convert to pandas
    
#     price_df=pd.DataFrame(list(price_dict.items()),columns = ['game_id','team_value'])
#     if count>30:
#         teams_data_list.append((team_name,price_df))
# import seaborn as sns
# import matplotlib.pyplot as plt

# # Check if team_data_list is not empty
# if teams_data_list:
#     # Initialize an empty list to store DataFrames
#     team_dataframes = []

#     # Iterate over each tuple in teams_data_list
#     for team_name, team_df in teams_data_list:
#         # Ensure the DataFrame has the expected column names and data types
#         if set(team_df.columns) == {"game_id", "team_value"}:
#             team_df["team_name"] = team_name  # Add a new column for team_name
#             team_dataframes.append(team_df)

#     # Check if there is any valid team data
#     if team_dataframes:
#         # Concatenate all team DataFrames into a single DataFrame
#         combined_data = pd.concat(team_dataframes)

#         # Plotting on the same axis
#         ax = sns.lineplot(x="game_id", y="team_value", hue="team_name", data=combined_data)
#         plt.xticks(rotation=90)
#         plt.title('Team Value Over Time')
#         plt.ylabel('Team Value')
#         plt.xlabel('#')
#         plt.legend(title='Team Name')

#         # Show the plot
#         plt.show()
#     else:
#         print("No valid team data available for plotting.")
# else:
#     print("No team data available for plotting.")
table='sports_action'
df= read_table_into_dataframe(pg_config, table)
#filter for data_source=sofasport
df = df[df['data_source']=='sofasport']
#fitler for action- Rating
df = df[df['action']=='Rating']

table='games_seasons'
games_seasons = read_table_into_dataframe(pg_config, table)


#plot df['action_count'] distribution
import seaborn as sns
import matplotlib.pyplot as plt
sns.set(style="whitegrid")
ax = sns.boxplot(x=df['action_count'])
plt.show()
