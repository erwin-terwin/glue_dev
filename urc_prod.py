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

ACTS=[]
for one_game in games_file_dict['games']:
    print("One game",one_game)
    list_of_games = one_game['endpoints']

    path_to_file =list_of_games['prefix']
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



    game_file = json.loads(read_s3_file(bucket_name, path_to_file) ).get('data',{})


    organization_dict={
        "id": organization_id,
        "name": organization_name}

    sport = {
            "id": sport_id,
            "name": sport_name,
            "games_supported": True
        }

    competition = {
            "id": competition_id,
            "name": competition_name,
            "sport_id": sport_id,
            "organization_id": organization_id,
        }

    season_dict={
                    "id":generate_uuid(game_file.get('season'),data_source=data_source) ,
                    "name": league_name,
                    "start_date": season_start_date,
                    "end_date": season_end_date,
                    'data_source': data_source,
                    "competition_id": competition_id,
                    }


    leagues_dict={
                        "id":generate_uuid(game_file.get('season'),data_source=data_source) ,
                        "name": league_name,
                        "season_id":generate_uuid(game_file.get('season'),data_source=data_source) ,
                        "start_date": season_start_date,
                        "end_date": season_end_date
                    }


    teams_dict_home={
        "athstat_id":generate_uuid(game_file.get('homeTeam').get("id"),data_source=data_source) ,
        "source_id":game_file.get('homeTeam').get("id") ,
        "athstat_name": game_file.get('homeTeam').get("name"),
        "data_source": data_source,
        "source_abbreviation": game_file.get('homeTeam').get("shortName"),
        "athstat_abbreviation": game_file.get('homeTeam').get("shortName"),
        "sport_id": sport_id,
        "organization_id": organization_id,
    }   

    teams_dict_away={
        "athstat_id":generate_uuid(game_file.get('awayTeam').get("id"),data_source=data_source) ,
        "source_id":game_file.get('awayTeam').get("id") ,
        "athstat_name": game_file.get('awayTeam').get("name"),
        "data_source": data_source,
        "source_abbreviation": game_file.get('awayTeam').get("shortName"),
        "athstat_abbreviation": game_file.get('awayTeam').get("shortName"),
        "sport_id": sport_id,
        "organization_id": organization_id,
    }
    all_teams = [teams_dict_home,teams_dict_away]

    games_seasons_dict={
        "game_id":generate_uuid(game_file.get('id'),data_source=data_source) ,
        "team_score": game_file.get('homeTeam').get("score"),
        "opposition_score": game_file.get('awayTeam').get("score"),
        "venue": game_file.get('venue').get("name"),
        "kickoff_time": game_file.get('date'),
        "competition_name": competition_name,
        "team_id":generate_uuid(game_file.get('homeTeam').get("id"),data_source=data_source) ,
        "opposition_team_id":generate_uuid(game_file.get('awayTeam').get("id"),data_source=data_source) ,
        "league_id":generate_uuid(game_file.get('season'),data_source=data_source) ,
        "round": game_file.get('round'),
        "game_status": game_file.get('status'),#map this
    }

    team_actions_list=[]
    for action_name in game_file.get('homeTeam').get("stats").keys():
        ACTS.append(action_name)
        action_name_ontology = action_ontology_dict.get(action_name,{})
        if action_name_ontology != {}:
            team_actions_list.append({
                "action": action_name_ontology,
                "action_count": game_file.get('homeTeam').get("stats").get(action_name),
                "game_id":generate_uuid(game_file.get('id'),data_source=data_source) ,
                "team_id":generate_uuid(game_file.get('homeTeam').get("id"),data_source=data_source) ,
            })
        else:
            logger.info(f'No mapping for {action_name}')
            continue
    for action_name in game_file.get('awayTeam').get("stats").keys():
        action_name_ontology = action_ontology_dict.get(action_name,{})
        if action_name_ontology != {}:
            team_actions_list.append({
                "action": action_name_ontology,
                "action_count": game_file.get('awayTeam').get("stats").get(action_name),
                "game_id":generate_uuid(game_file.get('id'),data_source=data_source) ,
                "team_id":generate_uuid(game_file.get('awayTeam').get("id"),data_source=data_source) ,
            })
        else:
            logger.info(f'No mapping for {action_name}')
            continue



    player_stats = []
    athlete_data=[]
    team_athletes=[]
    roster=[]
    list_of_teams=["homeTeam","awayTeam"]

    for team_obj in list_of_teams:
        for player in game_file.get(team_obj).get('players'):
            player_position_mapped = position_class_dict.get(player.get('position'),None)
            is_substitute=False
            if player_position_mapped=='replacement':
                is_substitute=True


            try:
                image_url=player.get('imageUrl')
                print(image_url)
                genetated_id=generate_uuid(player.get('id'),data_source=data_source)
                player_image_path=f'logos/{genetated_id}.png'
                player_image_data = requests.get(image_url).content
                s3_client.put_object(Body=player_image_data, Bucket='athstat-landing-assets-migrated', Key=player_image_path)
                S3_url = f'https://athstat-landing-assets-migrated.s3.amazonaws.com/{player_image_path}'
                print('Image uploaded to S3 !')
                print('\n')

            except Exception as e:
                print(e)
                print('Image not processed !')
                S3_url=None


                print(S3_url)
                

            athlete_dict={
                "source_id":player.get('id'),
                "tracking_id":generate_uuid(player.get('id'),data_source=data_source),
                "player_name":player.get('name'),
                "nick_name":player.get('known'),
                # "birth_country":None,
                # "date_of_birth":None,
                # "abbr":None,
                "athstat_name":player.get('name'),
                "athstat_firstname":player.get('firstName'),
                "athstat_lastname":player.get('lastName'),
                "athstat_middleinitial":None,
                "team_id":generate_uuid(game_file.get(team_obj).get("id"),data_source=data_source), 
                # "age":None,
                # "height":None,
                # "weight":None,
                "gender":"M",
                "position_class":None,
                "data_source":data_source,
                "position":player_position_mapped,#map thiscc
                'image_url':S3_url
                # "positionName":player.get('positionName'),#map this
            }
            athlete_data.append(athlete_dict)
            team_athletes.append({
                "team_id":generate_uuid(game_file.get(team_obj).get("id"),data_source=data_source), 
                "athlete_id":generate_uuid(player.get('id'),data_source=data_source),
            })
            roster_dict={
                        "player_number": player.get('positionId'),
                        "athlete_id": generate_uuid(player.get('id'),data_source=data_source),
                        "team_id": generate_uuid(game_file.get(team_obj).get("id"),data_source=data_source),
                        "game_id": generate_uuid(game_file.get('id'),data_source=data_source),
                        "position": player_position_mapped,
                        "is_substitute": is_substitute,
                    }
            roster.append(roster_dict)
            for action_name in player.get('stats'):
                action_name_ontology = action_ontology_dict.get(action_name,{})
                # if action name in nan then skip
                if action_name_ontology != {} and player.get('stats').get(action_name) != None:
                    player_stats.append({
                                'action': action_name_ontology,
                                'action_count': player.get('stats').get(action_name),
                                'game_id': generate_uuid(game_file.get('id'),data_source=data_source),
                                'team_id': generate_uuid(game_file.get(team_obj).get("id"),data_source=data_source),
                                'athlete_id': generate_uuid(player.get('id'),data_source=data_source),
                                "data_source": data_source,
                            })
    upsert_data(table='organizations', data_dict=organization_dict, conflict_ids='id',pg_config=pg_config)
    upsert_data(table='sports', data_dict=sport, conflict_ids='id',pg_config=pg_config)
    upsert_data(table='competitions', data_dict=competition, conflict_ids='id',pg_config=pg_config)
    upsert_data(table='seasons', data_dict=season_dict, conflict_ids='id',pg_config=pg_config)
    upsert_data(table='leagues', data_dict=leagues_dict, conflict_ids='id',pg_config=pg_config)
    upsert_data(table='teams', data_dict=teams_dict_home, conflict_ids='athstat_id',pg_config=pg_config)
    upsert_data(table='teams', data_dict=teams_dict_away, conflict_ids='athstat_id',pg_config=pg_config)
    upsert_data(table='games_seasons', data_dict=games_seasons_dict, conflict_ids='game_id',pg_config=pg_config)

    bulk_upsert_data(table='team_actions', data_dict=team_actions_list, conflict_ids=['game_id', 'team_id', 'action'],pg_config=pg_config)
    bulk_upsert_data(table='athletes',data_dict= athlete_data, conflict_ids='tracking_id',pg_config=pg_config)
    bulk_upsert_data(table='teams_athletes', data_dict=team_athletes, conflict_ids=['athlete_id', 'team_id'],pg_config=pg_config)
    bulk_upsert_data(table='game_roster', data_dict=roster, conflict_ids=['athlete_id', 'game_id', 'team_id'],pg_config=pg_config)
    bulk_upsert_data(table='sports_action', data_dict=player_stats, conflict_ids=['game_id', 'team_id', 'athlete_id', 'action'],pg_config=pg_config)


list(set(ACTS))