
"""
Athstat ETL S3 to Postgres RDS

This script extracts data from S3, transforms it, and loads it into a PostgreSQL RDS instance.

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
# Initialize Glue client
glue_client = boto3.client('glue', region_name='us-east-1')
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
s3_client = boto3.client('s3')
#list the files in the location
bucket_name='athstat-etl-migrated'



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




#Load the yaml file with the games
games_dictionary = yaml.safe_load(read_s3_file(bucket_name=bucket_name, file_name='data_maps/mlr_games.yaml'))
#take first key



standard_action_names_path="data_maps/rugby_actions_mapping.csv"
standard_action_names_file=read_s3_file(bucket_name=bucket_name, file_name=standard_action_names_path)
standard_action_names_df=pd.read_csv(io.StringIO(standard_action_names_file),sep=',')
data_source_column='MLR'

standard_pbp_action_names=standard_action_names_df
#fileter if Type == pbp
standard_pbp_action_names=standard_pbp_action_names[standard_pbp_action_names['Type']=='pbp']
#select data source column and action column
standard_pbp_action_names=standard_pbp_action_names[[data_source_column,'Athstat action']]
#convert to dictionary
standard_pbp_action_names_dict=standard_pbp_action_names.set_index(data_source_column).to_dict()['Athstat action']

standard_action_names_df=standard_action_names_df[[data_source_column,'Athstat action']]
standard_action_names_dict=standard_action_names_df.set_index(data_source_column).to_dict()['Athstat action']

#get game_status_mapping.csv from bucket and folder data_maps on S3 
game_status_mapping_path="data_maps/game_status_mapping.csv"
game_status_mapping_file=read_s3_file(bucket_name=bucket_name, file_name=game_status_mapping_path)
game_status_mapping_df=pd.read_csv(io.StringIO(game_status_mapping_file),sep=',')
#select data_source=mobii and athstat and athstat_abbreviation columns
game_status_mapping_df=game_status_mapping_df[['mobii','athstat','athstat_abbreviation']]
game_status_dict_df=game_status_mapping_df[['mobii','athstat']]
#create dict
game_status_dict=game_status_dict_df.set_index('mobii').to_dict()['athstat']


#read rugby_15s_position_ontology.csv
rugby_15s_position_ontology_path="data_maps/rugby_15s_position_ontology.csv"
rugby_15s_position_ontology_file=read_s3_file(bucket_name=bucket_name, file_name=rugby_15s_position_ontology_path)
rugby_15s_position_ontology_df=pd.read_csv(io.StringIO(rugby_15s_position_ontology_file),sep=',')
#choose mlr and athstat_position
rugby_15s_position_ontology_df=rugby_15s_position_ontology_df[['mlr','athstat_position']]
#create position_ontology_dict
position_ontology_dict=rugby_15s_position_ontology_df.set_index('mlr').to_dict()['athstat_position']

#------------------------------------------- processing the games -----------------------------------------------

game_ids=list(games_dictionary.keys())
# game_ids=game_ids[299:300]
#game_ids=['52d4ff39-a41b-472e-8ec8-a91f9c46186a']

counter=0
Failed_Files_List=[]
failure_dict={}
start_time=datetime.now()
for game_id in game_ids:
    print('\n')
    print('Processing game '+str(game_id))
    print('Game number '+str(counter)+' out of '+str(len(game_ids)))
    print('\n')
    try:




        #get match report into json
        match_report_json=json.loads(read_s3_file(bucket_name=bucket_name, file_name=games_dictionary[game_id]['match_report']))
        #get match results into json
        match_results_json=json.loads(read_s3_file(bucket_name=bucket_name, file_name=games_dictionary[game_id]['results']))
        #get year
        year=games_dictionary[game_id]['year']
        #match_timeline
        match_timeline_json=json.loads(read_s3_file(bucket_name=bucket_name, file_name=games_dictionary[game_id]['match_timeline']))
        #live_match_timeline
        #live_match_timeline_json=json.loads(read_s3_file(bucket_name=bucket_name, file_name=games_dictionary[game_id]['live_match_timeline']))
        

        #------------------------------------------- fixed values -----------------------------------------------
        sport_id=1
        sport_name='Rugby Union Men'
        data_source='mobii'
        league_name='Major League Rugby'
        competition_id=27 #set to 27, fixed competitoin ID from leagues tracked spreasheet
        organization_id=1 #changed to 1, MLR organization id is 1, World Rugby
        competition_name='Major League Rugby'


        # ------------------------------------------- extract game info -----------------------------------------------
        seasonName=match_results_json['seasonName']
        seasonId=match_results_json['seasonId']
        homeTeamId=match_results_json['homeTeamId']
        awayTeamId=match_results_json['awayTeamId']
        roundId=match_results_json['roundId']
        roundNumber=match_results_json['roundNumber']
        roundName=match_results_json['roundName']
        venueName=match_results_json['venueName']
        status=match_results_json['status']
        #map status to athstat status
        status=game_status_dict.get(status)


        referees=match_results_json['referees']
        seriesName=match_results_json['seriesName']
        teams=match_results_json['teams']
        dateString=match_results_json['dateString']
        date=match_report_json['date']
        teams=match_results_json['teams']
        team_0=teams[0]
        team_1=teams[1]
        team_names={}
        team_0_id=team_0['teamId']
        team_1_id=team_1['teamId']
        team_0_name=team_0['name']
        team_1_name=team_1['name']
        team_0_score=team_0['score']
        team_1_score=team_1['score']
        scores_dict={
            team_0_id:team_0_score,
            team_1_id:team_1_score
        }

        team_names[team_0_id]=team_0_name
        team_names[team_1_id]=team_1_name


        home_team_name=team_names[homeTeamId]
        away_team_name=team_names[awayTeamId]

        home_score=scores_dict[homeTeamId]
        away_score=scores_dict[awayTeamId]


        timeString=match_results_json['timeString']
        #convert time string to postgres time format
        timeString=datetime.strptime(timeString, '%H:%M:%S').time()
        #convert date string to postgres date format
        dateString=datetime.strptime(dateString, '%Y-%m-%d').date()
        kickoff_time=datetime.combine(dateString,timeString)







        #get match scores



        season_start_date=year
        season_end_date=year
        #covnert to postgres datetime format use 1 january as start date
        season_start_date=datetime.strptime(season_start_date+'-01-01','%Y-%m-%d')
        season_end_date=datetime.strptime(season_end_date+'-12-31','%Y-%m-%d')

        #make sport dictionary
        sport = {
            'id':sport_id,
            'name':sport_name,
            'games_supported':True
        }
        if year=='2023':
            hidden=True
        else:
            hidden=False

        #competition
        competition={
            "id":competition_id,
            "name":competition_name,
            "sport_id":sport_id,
            "organization_id":organization_id
        }


        #season dict
        season_dict={
            "id":generate_uuid(seasonId,data_source=data_source),
            "name":league_name+ ' ' +seasonName,
            "start_date":season_start_date,
            "end_date":season_end_date,
            "data_source":data_source,
            "competition_id":competition_id,
            "games_supported":True, #set to true, MLR is possible to play in athstat games QA/PROD
        }


        leagues_dict={
            "id":generate_uuid(seasonId,data_source=data_source), #not a good idea to use season id as league id
            "name":league_name,
            "season_id":generate_uuid(seasonId,data_source=data_source),
            "start_date":season_start_date,
            "end_date":season_end_date,

        }




        teams_dict_home={
            "athstat_id":generate_uuid(homeTeamId,data_source=data_source),
            "source_id":homeTeamId,
            "athstat_name":home_team_name,
            "data_source":data_source,
            "source_abbreviation":home_team_name, #work on abbreviations
            "athstat_abbreviation":home_team_name,
            "sport_id":sport_id,
            "organization_id":organization_id

        }

        teams_dict_away={
            "athstat_id":generate_uuid(awayTeamId,data_source=data_source),
            "source_id":awayTeamId,
            "athstat_name":away_team_name,
            "data_source":data_source,
            "source_abbreviation":away_team_name, #work on abbreviations
            "athstat_abbreviation":away_team_name,
            "sport_id":sport_id,
            "organization_id":organization_id

        }

        all_teams=[teams_dict_home,teams_dict_away]
        

        games_seasons_dict={
            "game_id":generate_uuid(game_id,data_source=data_source),
            "team_score":home_score,
            "opposition_score":away_score,
            "venue":venueName,
            "kickoff_time":kickoff_time,
            "competition_name":competition_name,
            "team_id":generate_uuid(homeTeamId,data_source=data_source),
            "opposition_team_id":generate_uuid(awayTeamId,data_source=data_source),
            "league_id":generate_uuid(seasonId,data_source=data_source),
            "round":roundNumber,
            "game_status":status,
            'result':True

        }


        # get team actions

        team_actions_list=[]
        team_0=match_report_json['teams'][0]
        team_1=match_report_json['teams'][1]
        team_0_id=team_0['teamId']
        team_1_id=team_1['teamId']

        stats_dict={
            team_0_id:{},
            team_1_id:{}
        }

        stats_dict[team_0_id]=team_0['metrics']
        stats_dict[team_1_id]=team_1['metrics']

        for team_id in stats_dict:
            team_metrics=stats_dict[team_id]
            for metric in team_metrics:
                action_name=metric.get('name')
                action_name_ontology=standard_action_names_dict.get(action_name)
                if action_name_ontology is not None and action_name_ontology != {}:
                    print(action_name_ontology)

                    team_actions_list.append({
                        "action":action_name_ontology,
                        "action_count":metric.get('value'),
                        "game_id":generate_uuid(game_id,data_source=data_source),
                        "team_id":generate_uuid(team_id,data_source=data_source),
                    })
                else:
                    logger.info(f'Action {action_name} not found in ontology')
                    continue


        player_stats_list=[]
        athlete_data=[]
        team_athletes=[]
        roster=[]


        team_stats=match_report_json['teams']
        team_A=team_stats[0]
        team_B=team_stats[1]
        team_A_id=team_A['teamId']
        team_B_id=team_B['teamId']
        A_players=team_A['players']
        B_players=team_B['players']

        player_stats_dict={
            team_0_id:{},
            team_1_id:{}
        }

        player_stats_dict[team_0_id]=A_players
        player_stats_dict[team_1_id]=B_players


        home_player_stats=player_stats_dict[homeTeamId]
        away_player_stats=player_stats_dict[awayTeamId]

        player_stats_dict={
            homeTeamId:{},
            awayTeamId:{}
        }

        player_stats_dict[homeTeamId]=home_player_stats
        player_stats_dict[awayTeamId]=away_player_stats

        for team_id in player_stats_dict.keys():
            print('Processing team '+str(team_id))
            player_stats=player_stats_dict[team_id]
            for stats in player_stats:
                #print('length of player stats '+str(len(player_stats)))
                athlete_dict={
                    "source_id":stats.get('playerId'),
                    "tracking_id":generate_uuid(stats.get('playerId'),data_source=data_source),
                    "player_name":stats.get('fullname'),
                    "nick_name":stats.get('displayName'),
                    "athstat_name":stats.get('fullname'),
                    "athstat_firstname":stats.get('firstName'),
                    "athstat_lastname":stats.get('lastName'),
                    "athstat_middleinitial":None,
                    "team_id":generate_uuid(team_id,data_source=data_source),
                    "gender":'M',
                    "position_class":None,#important one
                    "data_source":data_source,
                    "position":position_ontology_dict[stats.get('startingNumber')],#important one to map
                    #question how does something like replacement end up with an Xp rating?

                }

                athlete_data.append(athlete_dict)
                team_athletes.append({
                    "team_id":generate_uuid(team_id,data_source=data_source),
                    "athlete_id":generate_uuid(stats.get('playerId'),data_source=data_source),
                })

                roster_dict={
                    "_id":generate_uuid(generate_uuid(stats.get('playerId'),game_id),data_source=data_source),
                    "player_number":stats.get('startingNumber'),
                    "athlete_id":generate_uuid(stats.get('playerId'),data_source=data_source),
                    "team_id":generate_uuid(team_id,data_source=data_source),
                    "game_id":generate_uuid(game_id,data_source=data_source),
                    "position":stats.get('startingNumber'),
                }


                roster.append(roster_dict)
                player_metrics=stats.get('metrics')
                if player_metrics !=None:
                    for metric in player_metrics:
                        action_name=metric.get('name')
                        action_value=metric.get('value')
                        #if action value is float round to 2 decimal places
                        if isinstance(action_value,float):
                            action_value=round(action_value,2)
                        action_name_ontology=standard_action_names_dict.get(action_name)
                        if action_name_ontology is not None and action_name_ontology != {}:
                            player_stats_list.append({
                                    "action":action_name_ontology,
                                    "action_count":action_value, # there was a bug here !
                                    "game_id":generate_uuid(game_id,data_source=data_source),
                                    "athlete_id":generate_uuid(stats.get('playerId'),data_source=data_source),
                                    "team_id":generate_uuid(team_id,data_source=data_source),
                                    "data_source":data_source,
                                })
                    

        # processing match timeline
        team_pbp_actions_list=[]
        player_pbp_actions_list=[]
        sports_action_pbp_list=[]
        print('Processing match timeline')
        for timeline_event in match_timeline_json:  
                  
            event_id=generate_uuid(timeline_event.get('eventId'),data_source=data_source)
            source_event_id=timeline_event.get('eventId')
            action_name=timeline_event.get('name')
            #optionName=timeline_event.get('optionName')
            try:
                team_id=generate_uuid(timeline_event.get('team').get('teamId'),data_source=data_source)
            except:
                team_id=None
            try:
                team_name=timeline_event.get('team').get('name')
            except:
                team_name=None
            action_count=1
            action_name_ontology=standard_action_names_dict.get(action_name)

            if action_name_ontology is not None and action_name_ontology != {}:

                    game_date=dateString
                    

                    # Assuming you have a datetime.date object
                

                    # Convert to date string in 'YYYY-MM-DD' format
                    game_date = game_date.strftime('%Y-%m-%d')

                    game_timestamp=timeline_event.get('matchTimeMs')          
                    #convet to int
                    game_timestamp=int(game_timestamp)
                    matchTime=timeline_event.get('matchTime')


                


                    team_score=home_score
                    opposition_score=away_score
                    
                    team_pbp_dict={
                        "event_id":event_id,
                        "team_id":team_id,
                        "action":action_name_ontology,
                        "action_count":action_count,
                        "team_score":team_score,
                        "opposition_score":opposition_score,
                        "data_source":data_source,
                        "game_timestamp":game_timestamp,
                        "source_event_id":source_event_id,
                        "game_date":game_date,
                        "team_name":team_name,
                        "game_id":generate_uuid(game_id,data_source=data_source)

                    }

                    team_pbp_actions_list.append(team_pbp_dict)

                    PlayerOne=timeline_event.get('playerOne')
                    PlayerTwo=timeline_event.get('playerTwo')
                    Players=[PlayerOne,PlayerTwo]
                    for player_dict in Players:
                        if player_dict==None:
                            continue

                        playerId=generate_uuid(player_dict.get('playerId'),data_source=data_source)

                
                        #convert game_date to string
                        game_date=str(game_date)


                        player_pbp_dict={
                            "event_id":event_id,
                            "team_id":team_id,
                            "action":action_name_ontology,
                            "action_count":action_count,
                            "team_score":team_score,
                            "opposition_score":opposition_score,
                            "data_source":data_source,
                            "game_timestamp":game_timestamp,
                            "source_event_id":source_event_id,
                            "game_date":game_date,
                            "team_name":team_name,
                            "game_id":generate_uuid(game_id,data_source=data_source),
                            "athstat_name":player_dict.get('fullname'),
                            "player_id":playerId,
                            "game_id":generate_uuid(game_id,data_source=data_source)


                        }

                        player_pbp_actions_list.append(player_pbp_dict) 
                        #convert game_timestamp to in ms to uct time 
                        

                        # Assuming game_timestamp is a Unix timestamp in seconds
                        
                        # Convert to UTC datetime
                        from datetime import datetime, timezone

                        

                        # Convert timestamp to datetime object
                        kickoff_datetime_utc = datetime.utcfromtimestamp(game_timestamp).replace(tzinfo=timezone.utc)

                        # Convert datetime to string with 'Z' notation
                        formatted_datetime = kickoff_datetime_utc.strftime('%Y-%m-%dT%H:%M:%S.%fZ')


                        # Print the UTC time
                        # print("Kickoff Time (UTC):", kickoff_datetime_utc)



                        time_of_day=timeline_event.get('timeOfDay')
                       

                        # Convert datetime object to UTC and format as PostgreSQL timestamp with timezone
                        timestamp_pbp = time_of_day

                                            #sport action
                        sports_action_dict={

                            "athlete_id":playerId,
                            "game_id":generate_uuid(game_id,data_source=data_source),
                            "team_id":team_id,
                            "timestamp": timestamp_pbp,
                            "game_time":matchTime,#minutes
                            "action":action_name_ontology,
                            "action_count":1,
                            "team_score":int(timeline_event.get("homeTeamScore")),
                            "opposition_score":int(timeline_event.get("awayTeamScore"),),
                        }

                        sports_action_pbp_list.append(sports_action_dict)




        # add live match timeline
     
       

        upsert_data(table='sports', data_dict=sport, conflict_ids='id',pg_config=pg_config)
        upsert_data(table='competitions', data_dict=competition, conflict_ids='id',pg_config=pg_config)
        upsert_data(table='seasons', data_dict=season_dict, conflict_ids='id',pg_config=pg_config)
        upsert_data(table='leagues', data_dict=leagues_dict, conflict_ids='id',pg_config=pg_config)
        upsert_data(table='teams', data_dict=teams_dict_home, conflict_ids='athstat_id',pg_config=pg_config)
        upsert_data(table='teams', data_dict=teams_dict_away, conflict_ids='athstat_id',pg_config=pg_config)
        upsert_data(table='games_seasons', data_dict=games_seasons_dict, conflict_ids='game_id',pg_config=pg_config)
        bulk_upsert_data(table='team_actions', data_dict=team_actions_list, conflict_ids=['game_id', 'team_id', 'action'],\
        pg_config=pg_config)
        bulk_upsert_data(table='athletes',data_dict= athlete_data, conflict_ids='tracking_id',pg_config=pg_config)
        bulk_upsert_data(table='teams_athletes', data_dict=team_athletes, conflict_ids=['athlete_id', 'team_id'],pg_config=pg_config)
        bulk_upsert_data(table='game_roster', data_dict=roster, conflict_ids=['athlete_id', 'game_id', 'team_id'],pg_config=pg_config)

        bulk_upsert_data(table='sports_action', data_dict=player_stats_list, conflict_ids=['game_id', 'team_id', 'athlete_id', 'action'],\
        pg_config=pg_config)



        #insert team pbp actions
        #bulk_upsert_data(table='team_pbp_actions', data_dict=team_pbp_actions_list, conflict_ids=['event_id','game_id','team_id'],\
        #pg_config=pg_config)

        # #insert player pbp actions
        # bulk_upsert_data(table='player_pbp_actions', data_dict=player_pbp_actions_list, conflict_ids=['event_id','game_id','team_id','player_id'],\
        # pg_config=pg_config)

        #insert into sports action pbp conflict ids (athlete_id, game_id, team_id)
        bulk_upsert_data(table='sports_action_pbp', data_dict=sports_action_pbp_list, conflict_ids=['athlete_id','game_id','team_id','timestamp'],\
        pg_config=pg_config)


        # for a in player_stats_list:
        #     print('action count '+str(a['action_count']), ': action name '+str(a['action']))
    except Exception as e:
        print('Error processing game '+str(game_id))
        print(e)
        Failed_Files_List.append(game_id)
        failure_dict[game_id]=str(e)
        continue
    counter+=1

time_end=datetime.now()
time_elapsed=time_end-start_time
print('\n')
print('Time elapsed '+str(time_elapsed))
print('\n')
print('Done!')
print('\n')
for failed in Failed_Files_List:
    print('Failed to process game '+str(failed))


#write failed logs to s3
failed_logs='\n'.join(Failed_Files_List)
s3_client.put_object(Body=failed_logs, Bucket=bucket_name, Key='aws_glue_target/logs/mlr_failed_games.txt')

#save jsons to lical with game_id
#save mathc report


