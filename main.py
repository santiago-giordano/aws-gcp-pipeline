import os
import subprocess
import sys
import json
import pandas as pd
import boto3
import csv
from botocore.exceptions import NoCredentialsError
from google.cloud import bigquery
# Python==3.11.9

# Lista de archivos a descargar desde AWS
file_paths = ['disney_plus_titles.csv', 'netflix_titles.csv']

# Instalar requisitos
def install_requirements():
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])

install_requirements()

# Leer credenciales de GCP y AWS
with open('_credentials/gcp_credentials.json') as gcp_file:
    gcp_credentials = json.load(gcp_file)
with open('_credentials/aws_credentials.json') as aws_file:
    aws_credentials = json.load(aws_file)

# Configuración de las credenciales de GCP
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = '_credentials/gcp_credentials.json'

# Variables de GCP
project_id = gcp_credentials['project_id']
dataset_id = gcp_credentials['dataset_id']

# Configuración de las credenciales de AWS
aws_access_key_id = aws_credentials['aws_access_key_id']
aws_secret_access_key = aws_credentials['aws_secret_access_key']
bucket_name = aws_credentials['aws_bucket_name']

def detect_delimiter(file_name):
    with open(file_name, 'r', newline='', encoding='utf-8') as csvfile:
        try:
            dialect = csv.Sniffer().sniff(csvfile.readline(), delimiters=';,\t|')
            return dialect.delimiter
        except csv.Error:
            return ','

def download_csvs_and_convert_to_df(bucket_name, file_paths, aws_access_key_id, aws_secret_access_key):
    s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    dataframes = {}
    for file_path in file_paths:
        local_file_name = file_path.split('/')[-1]
        service_name = local_file_name[:-11] # local_file_name.split('_')[0]
        try:
            s3.download_file(bucket_name, file_path, local_file_name)
            delimiter = detect_delimiter(local_file_name)
            df = pd.read_csv(local_file_name, delimiter=delimiter, quotechar='"', escapechar='\\', on_bad_lines='skip')
            # Convertir date_added a formato de fecha
            df['date_added'] = pd.to_datetime(df['date_added'], format='%B %d, %Y', errors='coerce')
            df['date_added'] = pd.to_datetime(df['date_added']).dt.date
            # Convertir release_year a formato numérico
            df['release_year'] = pd.to_numeric(df['release_year'], downcast='integer', errors='coerce')
            # Eliminar comillas de cast
            df['cast'] = df['cast'].str.strip('"')
            # Crear columna unique_id
            df['unique_id'] = service_name + '_' + df['show_id'].astype(str)
            dataframes[local_file_name] = df
        except NoCredentialsError:
            print("Credenciales de AWS no disponibles o incorrectas.")
            break
        except Exception as e:
            print(f"Error: {str(e)}")
    return dataframes

def create_table_ddl(table_id):
    with open('queries/ddl.sql', 'r') as ddl_file:
        ddl = ddl_file.read().format(table_id=table_id)
    client = bigquery.Client()
    try:
        query_job = client.query(ddl)
        query_job.result()
        print("Tabla creada con éxito.")
    except Exception as e:
        print(f"Error al ejecutar DDL: {e}")

def load_data_to_bigquery(df, table_id):
    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job_config.schema = [
        bigquery.SchemaField("unique_id", "STRING"),
        bigquery.SchemaField("show_id", "STRING"),
        bigquery.SchemaField("type", "STRING"),
        bigquery.SchemaField("title", "STRING"),
        bigquery.SchemaField("director", "STRING"),
        bigquery.SchemaField("cast", "STRING"),
        bigquery.SchemaField("country", "STRING"),
        bigquery.SchemaField("date_added", "DATE"),
        bigquery.SchemaField("release_year", "INT64"),
        bigquery.SchemaField("rating", "STRING"),
        bigquery.SchemaField("duration", "STRING"),
        bigquery.SchemaField("listed_in", "STRING"),
        bigquery.SchemaField("description", "STRING"),
    ]
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    print(f'Data loaded to BigQuery table {table_id}')

def execute_union_query():
    with open('queries/union.sql', 'r') as query_file:
        query = query_file.read()
    client = bigquery.Client()
    try:
        query_job = client.query(query)
        query_job.result()
        print("Query ejecutada exitosamente y union table creada.")
    except Exception as e:
        print(f"Error ejecutando union query: {e}")

if __name__ == "__main__":
    # Descargar y convertir archivos CSV a DataFrames
    dataframes = download_csvs_and_convert_to_df(bucket_name, file_paths, aws_access_key_id, aws_secret_access_key)

    # Cargar datos en BigQuery
    for filename, df in dataframes.items():
        table_name = filename[:-4]
        table_id = f"{project_id}.{dataset_id}.{table_name}"
        create_table_ddl(table_id)
        load_data_to_bigquery(df, table_id)

    # Ejecutar la query de unión
    execute_union_query()