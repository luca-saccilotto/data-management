import boto3
import pandas as pd
import requests
import tabula

from botocore import UNSIGNED
from botocore.client import Config

class DataExtractor:

    def read_table(self, engine, table_name):
        user_data = pd.read_sql_table(table_name, engine)
        return user_data
    
    def retrieve_pdf(self, link):
        tables = tabula.read_pdf(link, lattice = True, pages = "all")
        card_details = pd.concat(tables)
        return card_details
    
    def list_stores(self, url, headers):
        response = requests.get(url, headers = headers)
        if response.status_code == 200:
            data = response.json()
            return data["number_stores"]
        else:
            return None
    
    def retrieve_stores(self, number_of_stores, url, headers):
        store_data = []
        for store in range(number_of_stores):
            response = requests.get(url + "/" + str(store), headers = headers)
            # Check and return stores details if response status code is successful
            if response.status_code == 200:
                store_data.append(list(response.json().values()))
                column_headings = response.json().keys()
            else:
                return None
        store_details = pd.DataFrame(data = store_data, columns = column_headings)
        return store_details
    
    def extract_s3(self, bucket_name, object_name, file_name):
        s3 = boto3.client("s3", config = Config(signature_version = UNSIGNED))
        s3.download_file(bucket_name, object_name, file_name)
        product_details = pd.read_csv(file_name)
        return product_details
    
    def extract_events(self, bucket_name, object_name, file_name):
        s3 = boto3.client("s3", config = Config(signature_version = UNSIGNED))
        s3.download_file(bucket_name, object_name, file_name)
        date_events = pd.read_json(file_name)
        return date_events
