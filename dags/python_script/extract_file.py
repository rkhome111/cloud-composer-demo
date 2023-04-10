from zipfile import ZipFile

def main():
    # zip_file = ZipFile('gs://rsk-bkt1/countries', 'r')
    # zip_file.extractall('gs://rsk-bkt1/UPRN/')
    print("Hello World!")
    x = 1
    if x == 1:
        # indented four spaces
        print("x is 1.")

if __name__ == "__main__":
    main()

def read_bigquery_response(ti):
    print(ti.xcom_pull(key='return_value',task_ids='get_data_from_bq'))