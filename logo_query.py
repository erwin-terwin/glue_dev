import requests
import psycopg2

# PostgreSQL connection parameters
db_params = {
    'dbname': 'sofasport',
    'user': 'postgres',
    'password': 'example',
    'host': 'localhost',
    'port': 5432
}

# Create a connection to the PostgreSQL database
conn = psycopg2.connect(**db_params)
cursor = conn.cursor()

# API URL and headers
url = "https://sofasport.p.rapidapi.com/v1/teams/data"
headers = {
    "X-RapidAPI-Key": "e6de28d4a4mshf8fb872f5381377p122f7ajsn2e37ed93790a",
    "X-RapidAPI-Host": "sofasport.p.rapidapi.com"
}

# Loop through team IDs and fetch data
# Loop through team IDs and fetch data
for team_id in range(1, 1001):
    querystring = {"team_id": team_id}
    response = requests.get(url, headers=headers, params=querystring)

    if response.status_code == 200:
        team_data = response.json()

        # Insert data into the PostgreSQL table (for selected fields only)
        cursor.execute("""
            INSERT INTO teams (
                id, name, slug, shortName, gender,
                sport_name, sport_slug, sport_id,
                category_name, category_slug, category_sport_name, category_sport_slug, category_sport_id, category_id,
                country_alpha2, country_name
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            team_data['data']['id'],
            team_data['data']['name'],
            team_data['data']['slug'],
            team_data['data']['shortName'],
            team_data['data']['gender'],
            team_data['data']['sport']['name'],
            team_data['data']['sport']['slug'],
            team_data['data']['sport']['id'],
            team_data['data']['category']['name'],
            team_data['data']['category']['slug'],
            team_data['data']['category']['sport']['name'],
            team_data['data']['category']['sport']['slug'],
            team_data['data']['category']['sport']['id'],
            team_data['data']['category']['id'],
            team_data['data']['country']['alpha2'],
            team_data['data']['country']['name']
        ))

        # Commit the transaction
        conn.commit()
        print(f"Team ID: {team_id} data successfully inserted into the PostgreSQL table.")
    else:
        print(f"Failed to fetch data for Team ID: {team_id}, Status Code: {response.status_code}")

# Close the cursor and connection
cursor.close()
conn.close()
