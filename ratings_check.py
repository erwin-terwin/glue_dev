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



#read from athlete_match_power_ranking
df = read_table_into_dataframe(pg_config, 'athlete_match_power_rankings')
#remove if updated_power_ranking=0
df = df[df['updated_power_ranking']!=0]

#plot rating distributiong i.e hostogram
import matplotlib.pyplot as plt
import seaborn as sns
sns.set(style="whitegrid")
#tips = sns.load_dataset("tips")
ax = sns.boxplot(x=df["updated_power_ranking"])
plt.show()


#plot histogram
ax = sns.histplot(df["updated_power_ranking"])
plt.show()

