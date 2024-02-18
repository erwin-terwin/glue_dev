import subprocess

def install_libraries():
    libraries = [
        'avro==1.11.0',
        'awscli==1.23.5',
        'awswrangler==2.15.1',
        'botocore==1.23.5',
        'boto3==1.22.5',
        'elasticsearch==8.2.0',
        'numpy==1.22.3',
        'pandas==1.4.2',
        'psycopg2==2.9.3',
        'pyathena==2.5.3',
        'PyGreSQL==5.0.6',
        'PyMySQL==1.0.2',
        'pyodbc==4.0.32',
        'pyorc==0.6.0',
        'redshift-connector==2.0.907',
        'requests==2.27.1',
        'scikit-learn==1.0.2',
        'scipy==1.8.0',
        'SQLAlchemy==1.4.36',
        's3fs==2022.3.0'
    ]

    for library in libraries:
        subprocess.run(['pip', 'install', library])

if __name__ == "__main__":
    install_libraries()
