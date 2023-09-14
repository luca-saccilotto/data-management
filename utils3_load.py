import os
import sqlalchemy as db
import yaml

class DatabaseConnector:

    def read_creds(self, file):
        with open(os.path.join("credentials", file), "r") as f:
            credentials = yaml.safe_load(f)
            return credentials
    
    def init_engine(self, credentials):
        engine = db.create_engine(f"{credentials['DATABASE_TYPE']}+{credentials['DBAPI']}://{credentials['USER']}:{credentials['PASSWORD']}@{credentials['HOST']}:{credentials['PORT']}/{credentials['DATABASE']}")
        return engine

    def list_tables(self, engine):
        inspector = db.inspect(engine)
        table_names = inspector.get_table_names()
        return table_names
    
    def upload_db(self, credentials, data, table_name):
        engine = db.create_engine(f"{credentials['DATABASE_TYPE']}+{credentials['DBAPI']}://{credentials['USER']}:{credentials['PASSWORD']}@{credentials['HOST']}:{credentials['PORT']}/{credentials['DATABASE']}")
        data.to_sql(table_name, engine, if_exists = "replace")
