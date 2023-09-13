import boto3
import pandas as pd
import requests
import tabula

from botocore import UNSIGNED
from botocore.client import Config

class DataExtractor:

    def read_table(self, engine, table_name):
        """
        This method reads the specified table into a dataframe using the database engine.
        
            Parameters:
                engine -> The database engine created using the credentials;
                table_name -> The name of the table to be read from the database.

            Returns:
                user_data -> A Pandas DataFrame containing the data from the specified table.
        
        """
        user_data = pd.read_sql_table(table_name, engine)
        return user_data
    
    def retrieve_pdf(self, link):
        """
       This method retrieves data from a PDF file located at the specified URL.

            Parameters:
                link -> The URL of the PDF file.

            Returns:
                card_details -> A Pandas DataFrame containing the data from the PDF file.
        
        """
        tables = tabula.read_pdf(link, lattice = True, pages = "all")
        card_details = pd.concat(tables)
        return card_details
    
    def list_stores(self, url, headers):
        """
        This method gets the number of stores from the given API endpoint.

            Parameters:
                url -> The URL of the API endpoint;
                headers -> A dictionary that contains the headers to send with the API request.

            Returns:
                data["number_stores"] -> The number of stores if the request is successful.
        
        """
        response = requests.get(url, headers = headers)
        if response.status_code == 200:
            data = response.json()
            return data["number_stores"]
        else:
            return None
    
    def retrieve_stores(self, number_of_stores, url, headers):
        """
        This method retrieves data for a specified number of stores from the given URL and headers.

            Parameters:
                number_of_stores -> The number of stores to retrieve data for;
                url -> The URL to retrieve the store data from;
                headers -> A dictionary containing the headers to use for the request.

            Returns:
                store_details -> A Pandas DataFrame containing the retrieved store data.
        
        """
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
        """
        This method downloads a file from an S3 bucket and returns its content as a dataset.
        
            Parameters:
                bucket_name -> The name of the S3 bucket;
                object_name -> The name of the S3 object;
                file_name -> The name of the local file to which the downloaded object should be saved.
            
            Returns:
                product_details -> A Pandas DataFrame containing the contents of the downloaded file.
        
        """
        s3 = boto3.client("s3", config = Config(signature_version = UNSIGNED))
        s3.download_file(bucket_name, object_name, file_name)
        product_details = pd.read_csv(file_name)
        return product_details
    
    def extract_events(self, bucket_name, object_name, file_name):
        """
        This method downloads a file from an S3 bucket and returns its content as a dataset.
        
            Parameters:
                bucket_name -> The name of the S3 bucket;
                object_name -> The name of the S3 object;
                file_name -> The name of the local file to which the downloaded object should be saved.
            
            Returns:
                date_events -> A Pandas DataFrame containing the contents of the downloaded file.
        
        """
        s3 = boto3.client("s3", config = Config(signature_version = UNSIGNED))
        s3.download_file(bucket_name, object_name, file_name)
        date_events = pd.read_json(file_name)
        return date_events