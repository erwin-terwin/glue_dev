import boto3
import os


#create boto3 client
s3_client = boto3.client('s3')
#create boto3 resource 
s3_resource = boto3.resource('s3')


#list all files in Countries folder on local
local_files = os.listdir('Countries')


S3_bucket='athstat-landing-assets-migrated'
subfolder='athstat_games_images/countries/'


for file in local_files:
    origin=os.path.join('Countries',file)
    #remove everything  before = and = in filename
    try:
        modified_name=file.split('=')[1]
    except:
        print('Failed to rename file: ',file)

    if '(' not in file:
        destination=os.path.join(subfolder,modified_name)
        s3_resource.meta.client.upload_file(origin,S3_bucket,destination)
        print('Moved file: ',file)