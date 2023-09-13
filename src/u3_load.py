import os
import sqlalchemy as db
import yaml

class DatabaseConnector:

    def read_creds(self, file):
        """
        This method reads database credentials from a YAML file and returns the
        as a dictionary.

            Parameters:
                file -> The file where the credentials are stored.

            Returns:
                credentials -> A dictionary containing the database credentials.

        """
        with open(os.path.join("credentials", file), "r") as f:
            credentials = yaml.safe_load(f)
            return credentials
    
    def init_engine(self, credentials):
        """
        This method reads the dictionary containing database credentials and connects
        to a database engine.

            Parameters:
                credentials -> A dictionary containing the database credentials.

            Returns:
                engine -> The database engine created using the credentials.

        """
        # Use the dictionary to set the database
        engine = db.create_engine(f"{credentials['DATABASE_TYPE']}+{credentials['DBAPI']}://{credentials['USER']}:{credentials['PASSWORD']}@{credentials['HOST']}:{credentials['PORT']}/{credentials['DATABASE']}")
        return engine

    def list_tables(self, engine):
        """
        This method returns a list of table names in the database.

            Parameters:
                engine -> The database engine created using the credentials.

            Returns:
                table_names -> The name of the tables inside the database.
        
        """
        inspector = db.inspect(engine)
        table_names = inspector.get_table_names()
        return table_names
    
    def upload_db(self, credentials, data, table_name):
        """
        This method connects to the database and uploads the data using the credentials
        stored in an external source.

            Parameters:
                credentials -> A dictionary containing the database credentials;
                df -> A Pandas DataFrame to upload to the database.

            Returns:
                This function does not return anything.

        """
        engine = db.create_engine(f"{credentials['DATABASE_TYPE']}+{credentials['DBAPI']}://{credentials['USER']}:{credentials['PASSWORD']}@{credentials['HOST']}:{credentials['PORT']}/{credentials['DATABASE']}")
        data.to_sql(table_name, engine, if_exists = "replace")