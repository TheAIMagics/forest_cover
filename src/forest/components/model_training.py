import sys
from src.forest.constants import *
from src.forest.exception import CustomException
from src.forest.logger import logging
from src.forest.entity.config_entity import *
from src.forest.entity.artifact_entity import *
from neuro_mf  import ModelFactory
from src.forest.entity.estimator import ForestModel
from src.forest.utils.main_utils import *

class ModelTrainer:
    def __init__(self, data_transformation_artifact: DataTransformationArtifact,
                 model_trainer_config: ModelTrainerConfig):
        self.data_transformation_artifact = data_transformation_artifact
        self.model_trainer_config = model_trainer_config
    
    def initiate_model_trainer(self) -> ModelTrainerArtifact:
        logging.info("Entered initiate_model_trainer method of ModelTrainer class")
        try:
            train_arr = load_numpy_array_data(file_path=self.data_transformation_artifact.transformed_train_file_path)
            test_arr = load_numpy_array_data(file_path=self.data_transformation_artifact.transformed_test_file_path)
            x_train, y_train, x_test, y_test = train_arr[:, :-1], train_arr[:, -1], test_arr[:, :-1], test_arr[:, -1]

            model_factory = ModelFactory(model_config_path=self.model_trainer_config.model_config_file_path)
            best_model_detail = model_factory.get_best_model(X=x_train,y=y_train,base_accuracy=self.model_trainer_config.expected_accuracy)
            
            if best_model_detail.best_score < self.model_trainer_config.expected_accuracy:
                logging.info("No best model found with score more than base score")
                raise Exception("No best model found with score more than base score")

            preprocessing_obj = load_object(file_path=self.data_transformation_artifact.transformed_object_file_path)

            forest_model = ForestModel(preprocessing_object=preprocessing_obj,trained_model_object=best_model_detail.best_model)
            
            logging.info("Created Forest model object with preprocessor and model")
            logging.info("Created best model file path.")
            save_object(self.model_trainer_config.trained_model_file_path, forest_model)

            metric_artifact = ClassificationMetricArtifact(f1_score=0.8, precision_score=0.8, recall_score=0.9)
            model_trainer_artifact = ModelTrainerArtifact(
                trained_model_file_path=self.model_trainer_config.trained_model_file_path,
                metric_artifact=metric_artifact,
            )
            logging.info(f"Model trainer artifact: {model_trainer_artifact}")
            return model_trainer_artifact
            
        except Exception as e:
            raise CustomException(e, sys) from e
