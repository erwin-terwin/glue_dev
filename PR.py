import boto3
import psycopg2
from psycopg2 import sql
from datetime import datetime
import io
import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
s3_client = boto3.client('s3')
from datetime import timedelta
import pandas as pd


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


bucket_name = "athstat-etl-migrated"
prefix = 'power_rankings/rugby-union-men-BR.csv'
print("Getting power rankings from S3")
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
def read_s3_file(bucket_name:str, file_name:str)->bytes:
    logger.info(f'Reading {file_name}')
    obj = s3_client.get_object(Bucket=bucket_name, Key=file_name)
    logger.info(f'File read')
    return obj['Body'].read()

power_rankings_bytes= read_s3_file(bucket_name=bucket_name, file_name=prefix)
power_rankings_bytes = io.BytesIO(power_rankings_bytes)
# power_rankings_bytes="rugby-union-BR.csv"
power_rankings_df = pd.read_csv(power_rankings_bytes)



power_rankings_df_dict = power_rankings_df.to_dict('records')
#for each field look for nan and replace with None
for i in range(len(power_rankings_df_dict)):
    for key in power_rankings_df_dict[i].keys():
        if pd.isna(power_rankings_df_dict[i][key]):
            power_rankings_df_dict[i][key] = None

rugby_position_ontology_bytes= read_s3_file(bucket_name=bucket_name, file_name="data_maps/rugby_15s_position_ontology.csv")
rugby_position_ontology_bytes = io.BytesIO(rugby_position_ontology_bytes)
# rugby_position_ontology_bytes="rugby_15s_position_ontology.csv"
rugby_position_ontology_df = pd.read_csv(rugby_position_ontology_bytes)
rugby_position_ontology_df=rugby_position_ontology_df.dropna(subset=["athstat_position"])
position_class_dict=rugby_position_ontology_df.set_index("athstat_position").to_dict()["position_group"]

bulk_upsert_data(table="power_ranking_coefficients", data_dict=power_rankings_df_dict, conflict_ids=["sport_id", "action_name","position"],pg_config=pg_config)
# breakpoint
def read_postgres_table(sql_query_string:str,pg_config:dict):
    """
    Read data from a PostgreSQL table.

    Args:
        sql_query_string (str): A SQL query string.
        pg_config (dict): A dictionary containing PostgreSQL connection parameters.

    Returns:
        list: A list of dictionaries containing the query results.

    Raises:
        Exception: If the SQL query string is empty.
        Exception: If the SQL query string is not a string.

    Note:

        This function uses the psycopg2 library to connect to a PostgreSQL database and execute a SQL query.
        The query results are returned as a list of dictionaries.
        
    """
    connection = psycopg2.connect(**pg_config)
    cursor = connection.cursor()
    cursor.execute(sql_query_string)
    query_result = cursor.fetchall()
    # Assuming cursor.description contains the column names
    column_names = [desc[0] for desc in cursor.description]

    # Convert the list of tuples to a list of dictionaries
    query_dict = [dict(zip(column_names, row)) for row in query_result]
    # Close the cursor and the connection
    cursor.close()
    connection.close()
    
    return query_dict



time_now_iso8601  = datetime.utcnow().isoformat()
time_minus_range = (datetime.utcnow() - timedelta(days=30)).isoformat()


# write a query to filter the table games_seasons where the kickoff_time is between time_now_iso8601 and time_24_hours_ago_iso8601

sql_query_string=f"SELECT * FROM games_seasons WHERE kickoff_time BETWEEN '{time_minus_range}' AND '{time_now_iso8601}' "

games_seasons_json=read_postgres_table(sql_query_string=sql_query_string,pg_config=pg_config)
for game_details in games_seasons_json:
    
    team_id =game_details["team_id"]
    game_id=game_details["game_id"]
    sql_query_string=f"SELECT * FROM teams WHERE athstat_id='{team_id}' "
    team_details=read_postgres_table(sql_query_string=sql_query_string,pg_config=pg_config)
    sport_id=team_details[0]["sport_id"]

    print("game_details",game_details)
    sql_query_string_roster=f"SELECT * FROM game_roster WHERE game_id='{game_id}'"
    game_roster=read_postgres_table(sql_query_string=sql_query_string_roster,pg_config=pg_config)
    
    if not game_roster:
      logger.error(f"The query returned no results. :{sql_query_string_roster}")
      continue

    else:
        logger.info("The query returned results.")
    list_of_player_details=[]
    for player in game_roster:
        power_ranking_total=0
        athlete_id=player["athlete_id"]
        player_position=player["position"]
        if player_position is None:
            print("player position is None")
            continue
        logger.info(f"position_class: {player_position} ")
        position_class=position_class_dict[player_position]
        sql_query_string=f"SELECT * FROM power_ranking_coefficients WHERE sport_id='{sport_id}'  AND position= '{position_class}' "
        power_ranking_json=read_postgres_table(sql_query_string=sql_query_string,pg_config=pg_config)
        for power_ranking_obj in power_ranking_json:
            action_name=power_ranking_obj["action_name"]
            sql_query_string_action=f"SELECT * FROM sports_action WHERE action='{action_name}' AND game_id='{game_id}' AND athlete_id='{athlete_id}'"
            sports_action_json=read_postgres_table(sql_query_string=sql_query_string_action,pg_config=pg_config)
            if len(sports_action_json)==0:
                player_action_count=0
            else:
                player_action_count=float(sports_action_json[0]["action_count"])
            
            impact =power_ranking_obj["impact"]
            five_star_value=power_ranking_obj["pr_90_100"]
            four_star_value=power_ranking_obj["pr_50_90"]
            three_star_value=power_ranking_obj["pr_25_50"]
            two_star_value=power_ranking_obj["pr_10_25"]
            one_star_value=power_ranking_obj["pr_0_10"]
            
            if impact =="Positive":
                if  five_star_value!=None and player_action_count >= five_star_value :
                    power_ranking_coefficient=power_ranking_obj["pr_5"] 
                elif four_star_value!=None and player_action_count >= four_star_value  :
                    power_ranking_coefficient=power_ranking_obj["pr_4"]
                elif  three_star_value!=None and player_action_count >= three_star_value :
                    power_ranking_coefficient=power_ranking_obj["pr_3"]
                elif two_star_value!=None and player_action_count >= two_star_value :
                    power_ranking_coefficient=power_ranking_obj["pr_2"]
                elif one_star_value!=None and player_action_count >= one_star_value :
                    power_ranking_coefficient=power_ranking_obj["pr_1"]
                else:
                    power_ranking_coefficient=power_ranking_obj["pr_1"]
                    print("Player actions did not meet any of the criteria")
                    
                    # breakpoint()
            elif impact =="Negative":
                if one_star_value!=None and player_action_count >= one_star_value   :
                    power_ranking_coefficient=power_ranking_obj["pr_1"]
                elif two_star_value!=None and  player_action_count >= two_star_value  :
                    power_ranking_coefficient=power_ranking_obj["pr_2"]
                elif three_star_value!=None and player_action_count >= three_star_value  :
                    power_ranking_coefficient=power_ranking_obj["pr_3"]
                elif four_star_value!=None and player_action_count >= four_star_value  :
                    power_ranking_coefficient=power_ranking_obj["pr_4"]
                elif five_star_value!=None and player_action_count >= five_star_value  :
                    power_ranking_coefficient=power_ranking_obj["pr_5"]
                else:
                    power_ranking_coefficient=power_ranking_obj["pr_5"]
                    print("Player actions did not meet any of the criteria")
                    # breakpoint()
            else:
                print("impact is not positive or negative")
                breakpoint()
            print("power_ranking_coefficient",power_ranking_coefficient)
            print("player_action_count",player_action_count)
            print("---->>Action name",action_name)
            print("Position",player_position)
            print("Position class",position_class)
            print("Player id",athlete_id)
            
            power_ranking_total+=power_ranking_coefficient
            print("power_ranking_total",power_ranking_total)
            print("*"*100)
            print("")
        # breakpoint()

        player_power_ranking_dict={
            "athlete_id":athlete_id,
            "game_id":game_id,
            "updated_power_ranking":int(power_ranking_total)
        }  
        list_of_player_details.append(player_power_ranking_dict)

        print(power_ranking_total)
    if not list_of_player_details:
        logger.error("list_of_player_details is empty.")
        continue
    else:
        logger.info("list_of_player_details is not empty.")
    try:
        bulk_upsert_data(table="athlete_match_power_rankings", data_dict=list_of_player_details, conflict_ids=["athlete_id","game_id"],pg_config=pg_config)
    except Exception as e:
        logger.error(e)
   