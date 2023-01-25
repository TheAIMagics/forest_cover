import os,sys
from src.forest.components.data_ingestion import DataIngestion
from src.forest.components.data_validation import DataValidation
from src.forest.components.data_transformation import DataTransformation
from src.forest.components.model_training import ModelTrainer
from src.forest.logger import logging
from src.forest.exception import CustomException
from src.forest.entity.config_entity import *
from src.forest.entity.artifact_entity import *

class TrainPipeline:
    def __init__(self):
        self.data_ingestion_config = DataIngestionConfig()
        self.data_validation_config = DataValidationConfig()
        self.data_transformation_config = DataTransformationConfig()
        self.model_trainer_config = ModelTrainerConfig()

        
    def start_data_ingestion(self)->DataIngestionArtifact:
        try:
            logging.info("Entered the start_data_ingestion method of TrainPipeline class")
            data_ingestion = DataIngestion(data_ingestion_config=self.data_ingestion_config)
            data_ingestion_artifact = data_ingestion.initiate_data_ingestion()
            logging.info("Exited the start_data_ingestion method of TrainPipeline class")
            return data_ingestion_artifact
        except Exception as e:
            raise CustomException(e,sys)


    def start_data_validation(self,data_ingestion_artifact : DataIngestionArtifact)-> DataValidationArtifact:
        try:
            logging.info("Entered the start_data_validation method of TrainPipeline class")
            data_validation = DataValidation(data_ingestion_artifact=data_ingestion_artifact,
            data_validation_config=self.data_validation_config)
            data_validation_artifact = data_validation.initiate_data_validation()
            logging.info("Exited the start_data_validation method of TrainPipeline class")
            return data_validation_artifact
        except Exception as e:
            raise CustomException(e,sys)

    def start_transformation(self,data_ingestion_artifact : DataIngestionArtifact) -> DataTransformationArtifact:
        try:
            logging.info("Entered the start_transformation method of TrainPipeline class")
            data_transformation = DataTransformation(data_ingestion_artifact=data_ingestion_artifact,
            data_transformation_config=self.data_transformation_config)
            data_transformation_artifact =  data_transformation.initiate_data_transformation()
            logging.info("Exited the start_transformation method of TrainPipeline class")
            return data_transformation_artifact
        except Exception as e:
            raise CustomException(e,sys)

    def start_model_trainer(self, data_transformation_artifact: DataTransformationArtifact) -> ModelTrainerArtifact:
        try:
            model_trainer = ModelTrainer(data_transformation_artifact=data_transformation_artifact,
                                         model_trainer_config=self.model_trainer_config)
            model_trainer_artifact = model_trainer.initiate_model_trainer()
            return model_trainer_artifact

        except Exception as e:
            raise CustomException(e, sys)

    
    def run_pipeline(self):
        data_ingestion_artifact = self.start_data_ingestion()
        data_validation_artifact = self.start_data_validation(data_ingestion_artifact=data_ingestion_artifact)
        if data_validation_artifact.validation_status:
            data_transformation_artifact = self.start_transformation(data_ingestion_artifact=data_ingestion_artifact)
            model_trainer_artifact = self.start_model_trainer(data_transformation_artifact=data_transformation_artifact)
        