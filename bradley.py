import boto3
import requests
import json
import os
import time

def download_image(url, save_path, directory):
    try:
        if not os.path.exists(directory):
            os.makedirs(directory)
        full_path = os.path.join(directory, save_path)
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            with open(full_path, 'wb') as f:
                f.write(response.content)
            print(f"Downloaded image: {full_path}")
            time.sleep(3)
        else:
            print(f"Failed to download image: {url} with status code: {response.status_code} and response: {response.text}")
    except Exception as e:
        print(f"Error downloading image: {url}. Error: {e}")


print("Script started")
bucket_name = 'athstat-etl-migrated'
prefix = '0-data-raw/api-collected/urc/game_reports/'
download_directory = 'downloaded_images'

s3 = boto3.client('s3')

try:
    response = s3.list_objects(Bucket=bucket_name, Prefix=prefix)

    if 'Contents' in response:
        for file in response['Contents']:
            file_key = file['Key']
            if file_key.endswith('.json'):
                json_file = s3.get_object(Bucket=bucket_name, Key=file_key)
                json_content = json_file['Body'].read().decode('utf-8')
                data = json.loads(json_content)
                data=data.get('data')
                home_team = data.get('homeTeam')
                away_team = data.get('awayTeam')
                home_team_imageUrl=home_team.get('imageUrl')
                away_team_imageUrl=away_team.get('imageUrl')
                home_team_imageUrls=home_team.get('imageUrls').get('ON DARK')
                away_team_imageUrls=away_team.get('imageUrls').get('ON DARK')
                home_team_DEFAULT_IMAGE=home_team.get('DEFAULT')
                away_team_DEFAULT_IMAGE=away_team.get('DEFAULT')
                #donwload 
                download_image(home_team_imageUrl, home_team_imageUrl.split('/')[-1], download_directory)
                download_image(away_team_imageUrl, away_team_imageUrl.split('/')[-1], download_directory)
                for image_url in home_team_imageUrls:
                    download_image(image_url, image_url.split('/')[-1], download_directory)
                for image_url in away_team_imageUrls:
                    download_image(image_url, image_url.split('/')[-1], download_directory)
                download_image(home_team_DEFAULT_IMAGE, home_team_DEFAULT_IMAGE.split('/')[-1], download_directory)
                download_image(away_team_DEFAULT_IMAGE, away_team_DEFAULT_IMAGE.split('/')[-1], download_directory)
    else:

        print("No files found in the specified bucket and prefix.")
                

        print("No JSON files found in the specified bucket and prefix.")
except Exception as e:
    print(f"Error accessing S3 or processing files: {e}")
