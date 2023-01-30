import os, sys
import pymongo
from src.forest.constants.database import *
from src.forest.exception import CustomException
import certifi
ca = certifi.where()

class MongoDBClient:
    client = None
    def __init__(self, database_name:DATABASE_NAME):
        try:
            if MongoDBClient.client is None:
                mongo_db_url = os.getenv('MONGODB_URL')
                if mongo_db_url is None:
                    raise Exception("Environment key for MONGODB_URL is not set")
                MongoDBClient.client = pymongo.MongoClient(mongo_db_url, tlsCAFile=ca)

            self.client = MongoDBClient.client
            self.database = self.client[database_name]
            self.database_name  = database_name
            
        except Exception as e:
            raise CustomException(e, sys)
