import sys
import numpy as np
import pandas as pd
from pandas import DataFrame
from src.forest.cloud_storage.aws_storage import SimpleStorageService
from src.forest.exception import CustomException
from src.forest.logger import logging
from src.forest.entity.config_entity import PredictionPipelineConfig
from src.forest.entity.s3_estimator import ForestEstimator

from src.forest.utils.main_utils import *


class PredictionPipeline:
    def __init__(self,prediction_pipeline_config:PredictionPipelineConfig=PredictionPipelineConfig(),)->None:
        """
        :param prediction_pipeline_config:
        """
        try:
            self.prediction_pipeline_config = prediction_pipeline_config
            self.s3 = SimpleStorageService()
        except Exception as e:
            raise CustomException(e,sys)
    
    def get_data(self,)->DataFrame:
        try:
            logging.info("Entered get_data method of PredictionPipeline class")
            
            prediction_df: DataFrame = self.s3.read_csv(filename=self.prediction_pipeline_config.data_file_path,
                                                        bucket_name=self.prediction_pipeline_config.data_bucket_name)
            logging.info("Read prediction csv file from s3 bucket")
            logging.info("Exited the get_data method of PredictionPipeline class")
            return prediction_df
        except Exception as e:
            raise CustomException(e, sys)

    

    def predict(self,dataframe)->np.ndarray:
        try:
            logging.info("Entered predict method of PredictionPipeline class")
            '''model = ForestEstimator(bucket_name=self.prediction_pipeline_config.model_bucket_name,
                                    model_path=self.prediction_pipeline_config.model_file_path)
'''
            trained_model = load_object(file_path=self.prediction_pipeline_config.trained_model_file_path)                       
            logging.info("Exited the predict method of PredictionPipeline class")
            return trained_model.predict(dataframe)
        except Exception as e:
            raise CustomException(e,sys)


    def initiate_prediction(self,)->None:
        try:
            logging.info("Entered initiate_prediction method of PredictionPipeline class")
            dataframe = self.get_data()
            predicted_arr = self.predict(dataframe)
            prediction = pd.DataFrame(list(predicted_arr))
            prediction.columns = ['Cover_Type'] 
            predicted_dataframe = pd.concat([dataframe,prediction],axis=1)
            
            self.s3.upload_df_as_csv(
                predicted_dataframe,
                self.prediction_pipeline_config.output_file_name,
                self.prediction_pipeline_config.output_file_name,
                self.prediction_pipeline_config.data_bucket_name,
            )
            logging.info("Uploaded artifacts folder to s3 bucket_name")

            logging.info(f"File has uploaded to {predicted_dataframe}")

            logging.info("Exited initiate_prediction method of PredictionPipeline class")
            return predicted_dataframe
        except Exception as e:
            raise CustomException(e,sys)




