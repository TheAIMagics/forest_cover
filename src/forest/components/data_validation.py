import os,sys
import json
import pandas as pd
from src.forest.logger import logging
from src.forest.exception import CustomException
from src.forest.entity.config_entity import *
from src.forest.entity.artifact_entity import *
from src.forest.constants import *
from src.forest.utils.main_utils import *
from evidently.model_profile import Profile
from evidently.model_profile.sections import DataDriftProfileSection

class DataValidation:
    def __init__(self, data_ingestion_artifact = DataIngestionArtifact,
    data_validation_config = DataValidationConfig):
        self.data_ingestion_artifact = data_ingestion_artifact
        self.data_validation_config = data_validation_config
        self._schema_config = read_yaml_file(file_path=SCHEMA_FILE_PATH)

    @staticmethod
    def read_data(filepath) ->pd.DataFrame:
        try:
            return pd.read_csv(filepath)
        except Exception as e:
            raise CustomException(e, sys)

    def validate_number_of_columns(self, dataframe: pd.DataFrame) -> bool:
        """
        :param dataframe:
        :return: True if all required columns present
        """
        try:
            status = len(dataframe.columns) == len(self._schema_config["columns"])
            logging.info(f"Is required column present: [{status}]")
            return status
        except Exception as e:
            raise CustomException(e, sys)

    def is_numerical_column_exist(self, dataframe: pd.DataFrame) -> bool:
        """
        This function check all the numerical column is present in dataframe or not
        :param df:
        :return: True if all column presents else False
        """
        try:
            dataframe_columns = dataframe.columns
            status = True
            missing_numerical_columns = []
            for column in self._schema_config["numerical_columns"]:
                if column not in dataframe_columns:
                    status = False
                    missing_numerical_columns.append(column)
            logging.info(f"Missing numerical column: {missing_numerical_columns}")
            return status
        except Exception as e:
            raise CustomException(e, sys) from e
    
    def detect_dataset_drift(self, reference_df: pd.DataFrame, current_df: pd.DataFrame, ) -> bool:
        """
        Method Name :   detect_dataset_drift
        Description :   This method validates if drift is detected
        
        Output      :   Returns bool value based on validation results
        On Failure  :   Write an exception log and then raise an exception
        """
        try:
            data_drift_profile = Profile(sections=[DataDriftProfileSection()])

            data_drift_profile.calculate(reference_df, current_df)

            report = data_drift_profile.json()
            json_report = json.loads(report)

            write_yaml_file(file_path=self.data_validation_config.drift_report_file_path, content=json_report)

            n_features = json_report["data_drift"]["data"]["metrics"]["n_features"]
            n_drifted_features = json_report["data_drift"]["data"]["metrics"]["n_drifted_features"]

            logging.info(f"{n_drifted_features}/{n_features} drift detected.")
            drift_status = json_report["data_drift"]["data"]["metrics"]["dataset_drift"]
        
            return drift_status
        
        except Exception as e:
            raise CustomException(e, sys) from e

    def initiate_data_validation(self):
        logging.info("Entered initiate_data_validation method of Data_Validation class")
        try:
            validation_error_msg = ''
            train_df, test_df = (DataValidation.read_data(filepath=self.data_ingestion_artifact.trained_file_path),
            (DataValidation.read_data(filepath=self.data_ingestion_artifact.test_file_path)))

            status = self.validate_number_of_columns(dataframe=train_df)
            if not status:
                validation_error_msg += "Columns are missing in train dataframe"

            status = self.validate_number_of_columns(dataframe=test_df)
            if not status:
                validation_error_msg += "Columns are missing in Test dataframe"

            status = self.is_numerical_column_exist(dataframe=train_df)
            if not status:
                validation_error_msg += "Columns are missing in train dataframe"

            status = self.is_numerical_column_exist(dataframe=test_df)
            if not status:
                validation_error_msg += "Columns are missing in Test dataframe"

            validation_status = len(validation_error_msg) == 0

            '''if validation_status:
                drift_status = self.detect_dataset_drift(train_df, test_df)
                if drift_status:
                    logging.info("Data Drift Detected")
            else:
                logging.info(f"Validation Error",{validation_error_msg})'''
            
            data_validation_artifact = DataValidationArtifact(
                validation_status=validation_status,
                message= validation_error_msg,
                drift_report_file_path=self.data_validation_config.drift_report_file_path
            )
            
            logging.info(f"Data validation artifact: {data_validation_artifact}")
            return data_validation_artifact

        except Exception as e:
            raise CustomException(e, sys)