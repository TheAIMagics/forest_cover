import sys
import numpy as np
import pandas as pd
from imblearn.combine import SMOTEENN
from src.forest.exception import CustomException
from src.forest.logger import logging
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.compose import ColumnTransformer
from src.forest.constants import *
from src.forest.entity.config_entity import *
from src.forest.entity.artifact_entity import *
from src.forest.utils.main_utils import save_object, save_numpy_array_data,read_yaml_file

class DataTransformation:
    def __init__(self, data_ingestion_artifact: DataIngestionArtifact,
    data_transformation_config : DataTransformationConfig):
        self.data_ingestion_artifact=data_ingestion_artifact
        self.data_transformation_config =  data_transformation_config
    
    def get_data_transformer_object(self) -> object:
        """
        Method Name :   get_data_transformer_object
        Description :   This method creates and returns a data transformer object 
        
        Output      :   data transformer object is created and returned 
        On Failure  :   Write an exception log and then raise an exception
        
        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        logging.info("Entered get_data_transformer_object method of DataTransformation class")

        try:
    
            _schema_config = read_yaml_file(file_path=SCHEMA_FILE_PATH)
            
            num_features = _schema_config['numerical_columns']

            numeric_pipeline = Pipeline(steps=[
                ('imputer', SimpleImputer(strategy='mean')),
                ('scaler', StandardScaler())
            ])
            preprocessor = ColumnTransformer(
                [
                    ("Numeric_Pipeline",numeric_pipeline,num_features)
                ]
            )
            logging.info("Created preprocessor object from ColumnTransformer")

            return preprocessor

        except Exception as e:
            raise CustomException(e, sys) from e

    @staticmethod
    def read_data(file_path) -> pd.DataFrame:
        try:
            return pd.read_csv(file_path)
        except Exception as e:
            raise CustomException(e, sys)

    def initiate_data_transformation(self)->DataTransformationArtifact:
        try:
            preprocessor = self.get_data_transformer_object()
            
            train_df = DataTransformation.read_data(file_path=self.data_ingestion_artifact.trained_file_path)
            test_df = DataTransformation.read_data(file_path= self.data_ingestion_artifact.test_file_path)
            logging.info("Got train features and test features of Training dataset")

            input_feature_train_df = train_df.drop(columns=[TARGET_COLUMN],axis=1)
            target_feature_train_df = train_df[TARGET_COLUMN]
            logging.info("Got input_features and target feature of Training dataset")

            input_feature_test_df = test_df.drop(columns=[TARGET_COLUMN],axis=1)
            target_feature_test_df = test_df[TARGET_COLUMN]
            logging.info("Got input_features and target feature of Test dataset")

            input_feature_train_arr = preprocessor.fit_transform(input_feature_train_df)
            logging.info("Used the preprocessor object to fit transform the train features")

            input_feature_test_arr = preprocessor.transform(input_feature_test_df)
            logging.info("Used the preprocessor object to fit transform the test features")

            smt = SMOTEENN(sampling_strategy='minority')

            '''input_feature_train_final, target_feature_train_final = smt.fit_resample(
                input_feature_train_arr, target_feature_train_df
            )'''

            logging.info("Applied SMOTEENN on training dataset")

            logging.info("Applying SMOTEENN on testing dataset")

            '''input_feature_test_final, target_feature_test_final = smt.fit_resample(
                input_feature_test_arr, target_feature_test_df
            )
'''
            logging.info("Applied SMOTEENN on testing dataset")

            train_arr = np.c_[input_feature_train_arr, np.array(target_feature_train_df)]
            
            test_arr = np.c_[input_feature_test_arr, np.array(target_feature_test_df)]

            save_object(self.data_transformation_config.transformed_object_file_path, preprocessor)
            save_numpy_array_data(self.data_transformation_config.transformed_train_file_path, array=train_arr)
            save_numpy_array_data(self.data_transformation_config.transformed_test_file_path, array=test_arr)

            logging.info("Saved the preprocessor object")

            data_transformation_artifact = DataTransformationArtifact(
                transformed_object_file_path=self.data_transformation_config.transformed_object_file_path,
                transformed_train_file_path=self.data_transformation_config.transformed_train_file_path,
                transformed_test_file_path=self.data_transformation_config.transformed_test_file_path
            )
            print(data_transformation_artifact)
            return data_transformation_artifact

        except Exception as e:
            raise CustomException(e, sys)
    