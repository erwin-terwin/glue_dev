import boto3
import psycopg2
from psycopg2 import sql
import json
import yaml
import uuid
import pandas as pd
import io
# Initialize Glue client
glue_client = boto3.client('glue', region_name='us-east-1')
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
s3_client = boto3.client('s3')
#list the files in the location
bucket_name='athstat-etl-migrated'


# Set the database to write to, Prod, QA or Local
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






#----------------------------------- Functions ------------------------------------------------------------------


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




#set location of yaml file

yaml_file_path='data_maps/sage_monitored_games.yaml'
yaml_file = read_s3_file(bucket_name, yaml_file_path)
games_file_dict = yaml.safe_load(yaml_file)

position_ontology_path="data_maps/rugby_actions_mapping.csv"
position_ontology_file = read_s3_file(bucket_name, position_ontology_path)
position_ontology_df = pd.read_csv(io.StringIO(position_ontology_file), sep=',')
data_source_column ="Sage"

position_ontology_df=position_ontology_df[[data_source_column,'Athstat action']]
position_ontology_df=position_ontology_df.dropna(subset=[data_source_column])
position_ontology_dict=position_ontology_df.set_index(data_source_column).to_dict()["Athstat action"]

number_of_games=len(games_file_dict['games'])

for game_number in range(number_of_games):
    print('Processing game number: ',game_number+1)
    list_of_games = games_file_dict['games'][game_number]['endpoints']
    path_to_file =list_of_games['prefix']
    competition_id = list_of_games['competition_id']
    competition_name = list_of_games['competition_name']
    data_source = list_of_games['data_source']
    league_name = list_of_games['league_name']
    organization_id = list_of_games['organization_id']
    season_end_date = list_of_games['season_end'].replace("‘", "'").replace("’", "'")
    season_start_date = list_of_games['season_start'].replace("‘", "'").replace("’", "'")
    sport_id = list_of_games['sport_id']
    sport_name = list_of_games['sport_name']


    #get game_id
    game_id = list_of_games['game_id']
    player_file=json.loads(read_s3_file(bucket_name, path_to_file+game_id+'_player_stats.json'))
    team_file=json.loads(read_s3_file(bucket_name, path_to_file+game_id+'_team_stats.json'))
    player_file=player_file['data']
    team_file=team_file['data']



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


    season_dict=player_file.get('season')
    season_guid=season_dict['guid']


    #season Dictionary
    season_dict={
                    "id":generate_uuid(season_guid,data_source=data_source) ,
                    "name": league_name,
                    "start_date": season_start_date,
                    "end_date": season_end_date,
                    'data_source': data_source,
                    "competition_id": competition_id,
                    }

    #league dictionary, map league id, Issue with league id !!!!!
    leagues_dict={
                        "id":generate_uuid(season_guid,data_source=data_source) ,
                        "name": league_name,
                        "season_id":generate_uuid(season_guid,data_source=data_source) ,
                        "start_date": season_start_date,
                        "end_date": season_end_date
                    }


    teams_dict_home={
        "athstat_id":generate_uuid(team_file.get('teams').get('teamA').get("guid"),data_source=data_source) ,
        "source_id":team_file.get('teams').get('teamA').get("guid") ,
        "athstat_name": team_file.get('teams').get('teamA').get("name"),
        "data_source": data_source,
        "source_abbreviation": team_file.get('teams').get('teamA').get("shortName"),
        "athstat_abbreviation": team_file.get('teams').get('teamA').get("shortName"),
        "sport_id": sport_id,
        "organization_id": organization_id,
    }   


    teams_dict_away={
        "athstat_id":generate_uuid(team_file.get('teams').get('teamB').get("guid"),data_source=data_source) ,
        "source_id":team_file.get('teams').get('teamB').get("guid") ,
        "athstat_name": team_file.get('teams').get('teamB').get("name"),
        "data_source": data_source,
        "source_abbreviation": team_file.get('teams').get('teamB').get("shortName"),
        "athstat_abbreviation": team_file.get('teams').get('teamB').get("shortName"),
        "sport_id": sport_id,
        "organization_id": organization_id,
    }


    all_teams=[teams_dict_home,teams_dict_away]



    # ..........................   sage transformations for players and teams  .......................................




    #games dict

    kick_off_time=list_of_games['game_start_date']
    games_seasons_dict={   
        "game_id":generate_uuid(game_id,data_source=data_source) ,
        "team_score": team_file.get('extra').get('teamA').get("Score"),
        "opposition_score": team_file.get('extra').get('teamB').get("Score"),
        "venue": None,#igore instead of none
        "kickoff_time": kick_off_time,
        "competition_name": competition_name,
        "team_id":generate_uuid(team_file.get('teams').get('teamA').get("guid"),data_source=data_source) ,
        "opposition_team_id":generate_uuid(team_file.get('teams').get('teamB').get("guid"),data_source=data_source) ,
        "league_id":generate_uuid(season_guid,data_source=data_source) ,
        "round": None,
        "game_status":"Full Time",#map this later !!!!!!!!!!!!!
    }

    print('Generated games seasons dictionaries')


    team_actions_list=[]

    for action_name in team_file.get('extra').get('teamA').keys():
        action_name_ontology = position_ontology_dict.get(action_name,{})
        if action_name_ontology != {}:
        

            team_actions_list.append({
                'action': action_name_ontology,
                'action_count': team_file.get('extra').get('teamA').get(action_name),
                'game_id': generate_uuid(game_id,data_source=data_source),
                'team_id': generate_uuid(team_file.get('teams').get('teamA').get("guid"),data_source=data_source),
            })
        else:
            logger.info(f'No action mapping for {action_name}')
            continue
    for action_name in team_file.get('extra').get('teamB').keys():
        action_name_ontology = position_ontology_dict.get(action_name,{})
        if action_name_ontology != {}:
            

            team_actions_list.append({
                'action': action_name_ontology,
                'action_count': team_file.get('extra').get('teamB').get(action_name),
                'game_id': generate_uuid(game_id,data_source=data_source),
                'team_id': generate_uuid(team_file.get('teams').get('teamB').get("guid"),data_source=data_source),
            })
        else:
            logger.info(f'No action mapping for {action_name}')
            continue


    player_stats = []
    athlete_data=[]
    team_athletes=[]
    roster=[]
    list_of_teams=['teamA','teamB']


    for team_obj in list_of_teams:
        for player in player_file.get('playerStats').get('playerStatistics').get(team_obj):
            athlete_dict={
            "source_id": player.get("player").get("guid"),
            "tracking_id":generate_uuid(player.get("player").get("guid"),data_source=data_source) ,
            "player_name": player.get("player").get("firstName")+" "+player.get("player").get("lastName"),
            "athstat_name": player.get("player").get("firstName")+" "+player.get("player").get("lastName"),
            "athstat_firstname": player.get("player").get("firstName"),
            "athstat_lastname": player.get("player").get("lastName"),
            "athstat_middleinitial":None, 
            "team_id":generate_uuid(team_file.get('teams').get(team_obj).get("guid"),data_source=data_source) ,
            "gender": None,
            "position_class": None,
            "data_source": data_source,
            "position": player.get("position"),
            }
            

            if team_obj=='teamA':
                team_id_player=generate_uuid(team_file.get('teams').get('teamA').get("guid"),data_source=data_source)
            else:
                team_id_player=generate_uuid(team_file.get('teams').get('teamB').get("guid"),data_source=data_source)

            athlete_data.append(athlete_dict)
            team_athletes.append({
                "team_id":team_id_player,
                "athlete_id":generate_uuid(player.get('player').get('guid'),data_source=data_source),
            })


            roster_dict={
                "player_number": player.get('position'),
                "athlete_id": generate_uuid(player.get('player').get('guid'),data_source=data_source),
                "team_id": team_id_player,
                "game_id": generate_uuid(game_id,data_source=data_source),
                "position": player.get('position'),
                # "is_substitute": False,
            }


            roster.append(roster_dict)
            temp_dict={}
            for actions in player.get('stats'): 
                
                action=actions.get('label')
                action=position_ontology_dict.get(action,{})
                action_value=actions.get('value')
                #convert to int
                try:
                    action_value=int(action_value)
                except:
                    pass

                if action != {}:
                    temp_dict[action]=action_value

            for action_name in temp_dict.keys():
                if action_name != {}:
                    player_stats.append({
                        "action": action_name,
                        "action_count": temp_dict[action_name],
                        "game_id": generate_uuid(game_id,data_source=data_source),
                        "team_id": team_id_player,
                        "athlete_id": generate_uuid(player.get('player').get('guid'),data_source=data_source),
                        'data_source': data_source,
                    })

                
                


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


    print('Compelted processing game id: ',game_id)
