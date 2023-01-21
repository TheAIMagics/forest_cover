import sys
import numpy as np
import pandas as pd
from src.forest.constants.database import *
from src.forest.exception import CustomException
from src.forest.configuration.mongo_db_connection import MongoDBClient
from typing import Optional

class ForestData:
    def __init__(self):
        try:
            self.mongo_client = MongoDBClient(database_name=DATABASE_NAME)
        except Exception as e:
            raise CustomException(e, sys)

    def export_collection_as_dataframe(self, collection_name : str, database_name : Optional[str]=None)->pd.DataFrame:
        try:
            if database_name is None:
                collection = self.mongo_client.database[collection_name]
            else:
                collection = self.mongo_client[database_name][collection_name]

            df = pd.DataFrame(list(collection.find()))

            if "_id" in list(df.columns):
                df = df.drop(columns=['_id'], axis=1)
            df.replace({'na':np.nan}, inplace=True)
            return df

        except Exception as e:
            raise CustomException(e, sys)