import os
from dataclasses import dataclass
from from_root import from_root
from src.forest.constants import *
from src.forest.constants import prediction_pipeline

@dataclass
class DataIngestionConfig:
    def __init__(self):
        self.data_ingestion_dir: str = os.path.join(from_root(), ARTIFACTS_DIR, DATA_INGESTION_ARTIFACTS_DIR)
        self.feature_store_file_path: str = os.path.join(self.data_ingestion_dir, DATA_INGESTION_FEATURE_STORE_DIR, FILE_NAME)
        self.training_file_path: str = os.path.join(self.data_ingestion_dir, DATA_INGESTION_INGESTED_DIR, TRAIN_FILE_NAME)
        self.testing_file_path: str = os.path.join(self.data_ingestion_dir, DATA_INGESTION_INGESTED_DIR, TEST_FILE_NAME)
        self.train_test_split_ratio: float = DATA_INGESTION_TRAIN_TEST_SPLIT_RATIO
    

@dataclass
class DataValidationConfig:
    def __init__(self):
        self.data_validation_dir: str = os.path.join(from_root(), ARTIFACTS_DIR, DATA_VALIDATION_DIR_NAME)
        self.drift_report_file_path: str = os.path.join(self.data_validation_dir, DATA_VALIDATION_DRIFT_REPORT_DIR,
                                               DATA_VALIDATION_DRIFT_REPORT_FILE_NAME)


@dataclass
class DataTransformationConfig:
    def __init__(self):
        self.data_transformation_dir: str = os.path.join(from_root(),ARTIFACTS_DIR, DATA_TRANSFORMATION_DIR_NAME)
        self.transformed_train_file_path: str = os.path.join(self.data_transformation_dir, DATA_TRANSFORMATION_TRANSFORMED_DATA_DIR,
                                                        TRAIN_FILE_NAME.replace("csv", "npy"))
        self.transformed_test_file_path: str = os.path.join(self.data_transformation_dir, DATA_TRANSFORMATION_TRANSFORMED_DATA_DIR,
                                                    TEST_FILE_NAME.replace("csv", "npy"))
        self.transformed_object_file_path: str = os.path.join(self.data_transformation_dir,
                                                        DATA_TRANSFORMATION_TRANSFORMED_OBJECT_DIR,
                                                        PREPROCSSING_OBJECT_FILE_NAME)

@dataclass
class ModelTrainerConfig:
    def __init__(self):
        self.model_trainer_dir: str = os.path.join(from_root(),ARTIFACTS_DIR, MODEL_TRAINER_DIR_NAME)
        self.trained_model_file_path: str = os.path.join(self.model_trainer_dir, MODEL_TRAINER_TRAINED_MODEL_DIR, MODEL_FILE_NAME)
        self.expected_accuracy: float = MODEL_TRAINER_EXPECTED_SCORE
        self.model_config_file_path: str = MODEL_TRAINER_MODEL_CONFIG_FILE_PATH

@dataclass
class ModelEvaluationConfig:
    def __init__(self):
        self.changed_threshold_score: float = MODEL_EVALUATION_CHANGED_THRESHOLD_SCORE
        self.bucket_name: str = MODEL_PUSHER_BUCKET_NAME
        self.s3_model_key_path: str = os.path.join(MODEL_PUSHER_S3_KEY, MODEL_FILE_NAME)

@dataclass
class ModelPusherConfig:
    bucket_name: str = MODEL_PUSHER_BUCKET_NAME
    s3_model_key_path: str = os.path.join(MODEL_PUSHER_S3_KEY, MODEL_FILE_NAME)

@dataclass
class PredictionPipelineConfig:
    data_bucket_name: str = prediction_pipeline.PREDICTION_DATA_BUCKET
    data_file_path: str = prediction_pipeline.PREDICTION_INPUT_FILE_NAME
    model_bucket_name: str = prediction_pipeline.MODEL_BUCKET_NAME
    model_file_path: str = os.path.join(MODEL_PUSHER_S3_KEY, MODEL_FILE_NAME) #/model-registry/model.pkl
    output_file_name:str = prediction_pipeline.PREDICTION_OUTPUT_FILE_NAME
    model_trainer_dir: str = os.path.join(from_root(),ARTIFACTS_DIR, MODEL_TRAINER_DIR_NAME)
    trained_model_file_path: str = os.path.join(model_trainer_dir, MODEL_TRAINER_TRAINED_MODEL_DIR, MODEL_FILE_NAME)
