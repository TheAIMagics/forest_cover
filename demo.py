from src.forest.pipeline.training_pipeline import TrainPipeline
from src.forest.pipeline.prediction_pipeline import PredictionPipeline

pipeline = TrainPipeline()
pipeline.run_pipeline()

'''prediction = PredictionPipeline()
prediction.initiate_prediction()'''