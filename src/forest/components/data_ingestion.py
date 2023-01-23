import os,sys
import pandas as pd
from src.forest.logger import logging
from src.forest.exception import CustomException
from src.forest.entity.config_entity import *
from src.forest.entity.artifact_entity import *
from src.forest.constants import *
from src.forest.utils.main_utils import *
from src.forest.data_access.forest_data import ForestData
from sklearn.model_selection import train_test_split
from src.forest.constants.database import *

class DataIngestion:
    def __init__(self, data_ingestion_config : DataIngestionConfig = DataIngestionConfig()):
        try:
            self.data_ingestion_config = data_ingestion_config
        except Exception as e:
            raise CustomException(e, sys)

    def export_data_into_feature_store(self)->pd.DataFrame:
        try:
            feature_store_file_path = self.data_ingestion_config.feature_store_file_path
            dir_path = os.path.dirname(feature_store_file_path)
            os.makedirs(dir_path,exist_ok=True)
            if os.path.exists(feature_store_file_path):
                dataframe = pd.read_csv(feature_store_file_path)

            else:
                logging.info(f"Exporting data from mongodb")
                forest_data = ForestData()
                dataframe = forest_data.export_collection_as_dataframe(collection_name=COLLECTION_NAME)
                logging.info(f"Shape of dataframe: {dataframe.shape}")
                
                logging.info(f"Saving exported data into feature store file path: {feature_store_file_path}")
                dataframe.to_csv(feature_store_file_path, index=False, header=True)
            
            return dataframe

        except Exception as e:
            raise CustomException(e, sys)

    def split_data_as_train_test(self, dataframe:pd.DataFrame):
        """
        Method Name :   split_data_as_train_test
        Description :   This method splits the dataframe into train set and test set based on split ratio 
        
        Output      :   Folder is created in s3 bucket
        On Failure  :   Write an exception log and then raise an exception
        
        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        try:
            train_set, test_set = train_test_split(dataframe, test_size = self.data_ingestion_config.train_test_split_ratio)
            logging.info("Performed train test split on the dataframe")
            dir_name = os.path.dirname(self.data_ingestion_config.training_file_path)
            os.makedirs(dir_name, exist_ok=True)
            train_set.to_csv(self.data_ingestion_config.training_file_path, index= False, header= True)
            test_set.to_csv(self.data_ingestion_config.testing_file_path, index= False, header= True)
        except Exception as e:
            raise CustomException(e, sys)

    def initiate_data_ingestion(self)->DataIngestionArtifact:
        """
        Method Name :   initiate_data_ingestion
        Description :   This method initiates the data ingestion components of training pipeline 
        
        Output      :   train set and test set are returned as the artifacts of data ingestion components
        On Failure  :   Write an exception log and then raise an exception
        
        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        try:
            dataframe = self.export_data_into_feature_store()
            _schema_config = read_yaml_file(file_path=SCHEMA_FILE_PATH)
            dataframe = dataframe.drop(_schema_config['drop_columns'], axis=1)
            self.split_data_as_train_test(dataframe=dataframe)
            data_ingestion_artifact = DataIngestionArtifact(
                trained_file_path=self.data_ingestion_config.training_file_path,
                test_file_path= self.data_ingestion_config.testing_file_path
            )
            return data_ingestion_artifact
        except Exception as e:
            raise CustomException(e, sys)