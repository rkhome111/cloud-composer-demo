from zipfile import ZipFile
import glob
import os
from shutil import rmtree
from google.cloud import storage
from google.cloud import bigquery
from google.oauth2 import service_account
#credentials = service_account.Credentials.from_service_account_file('/opt/airflow/dags/keys/burner-rousingh.json')
credentials = service_account.Credentials.from_service_account_file('../keys/burner-rousingh.json')

project_id = 'burner-rousingh'
TABLE = 'poc_dataset.data_load_config'
client = bigquery.Client(credentials= credentials,project=project_id)

def main():
    print("Hello World!")
    query = getConfiguration()
    query_job = client.query(query)
    config_results = query_job.result()
    
    for row in config_results:
        print(row.table_name, row.zip_file_name,row.text_file_name,row.primay_key, row.hql_file_name)
        createTable(row.hql_file_name)
        #method to load data from gcs bucket to big query
        #uploadData(row.text_file_name, row.table_name)

        #Below two methods are used to download zip file and upload it to bigquery
        downloadAndExtractZipFileToEdgeNode(row.zip_file_name)
        uploadCsvFileFromEdgeNodeToBQ(row.zip_file_name, row.text_file_name, row.table_name)
        hydrateTable(row.table_name)

def hydrateTable(table_name):
    client.query("UPDATE poc_dataset."+table_name+" SET startdate=current_date(),  enddate='9999-12-31' where true").result()
    print("table hydrated successfully "+table_name)

#upload csv file from gcs bucket to bigquery
def uploadData(text_file_name, table_name):
    job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV, skip_leading_rows=0, autodetect=False,allow_jagged_rows=True)
    job = client.load_table_from_uri("gs://rsk-bkt1/"+text_file_name+"*.csv", "poc_dataset."+table_name, job_config=job_config)
    job.result()
    print("successfully loaded from GCS Bucket to BQ: "+table_name)

#upload csv file from edge node to bigquery
def uploadCsvFileFromEdgeNodeToBQ(zip_file_name, text_file_name, table_name):
    job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV, skip_leading_rows=0, autodetect=False,allow_jagged_rows=True)
    for file_name in glob.glob(zip_file_name+"*/*.csv"):
        print("processing local file ------------"+file_name)
        with open(file_name, 'rb') as source_file:  
            job = client.load_table_from_file(source_file, "poc_dataset."+table_name, job_config=job_config)
        job.result()
    rmtree(zip_file_name)
    os.remove(zip_file_name+'.zip')
    print("successfully loaded from edge location csv file to BQ:"+table_name)

def createTable(hql_file_name):
    client.query(readGSFileAsString("sql/"+hql_file_name)).result()

def getConfiguration():
    return readGSFileAsString('sql/config_query.sql')
    
def readGSFileAsString(location):
    print(type(location))
    client = storage.Client(credentials= credentials)
    bucket = client.get_bucket('rsk-bkt1')
    blob = bucket.get_blob(location)
    return blob.download_as_string().decode('utf-8')

def downloadAndExtractZipFileToEdgeNode(zipFileName):
    print(type(zipFileName))
    client = storage.Client(credentials= credentials)
    bucket = client.get_bucket('rsk-bkt1')
    blob = bucket.blob(zipFileName+".zip")
    blob.download_to_filename(zipFileName+".zip")
    with ZipFile(zipFileName+".zip", 'r') as f:
        f.extractall(path=zipFileName)

if __name__ == "__main__":
    #print(createTable('uprn01_full.sql'))
    main()
    #downloadZipFileToEdgeNode("pointer_full")
    #print(glob.glob("addressbase_full/*.csv"))