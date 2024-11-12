from fastapi import FastAPI, HTTPException
from contextlib import asynccontextmanager
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pydantic import BaseModel  
from typing import List, Optional, Dict
import uvicorn
import logging
import os
import glob

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define Pydantic request and response models
class LoanRequest(BaseModel):
    Credit_Score: float
    LTV: float
    loan_amount: float

class PredictionResponse(BaseModel):
    prediction: float  # Prediction for loan approval (1: approved, 0: denied)
    prediction_details: Optional[dict] = None

# Define global variables
spark = None
models: Dict[str, PipelineModel] = {}

# Initialize Spark session
def initialize_spark():
    spark = SparkSession.builder \
        .appName("LoanApprovalPredictor") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    return spark

# Load models from directory
def load_models(base_model_path: str = "./models"):
    """Load all available models (model_1, model_2, model_3)"""
    model_dirs = glob.glob(os.path.join(base_model_path, "model_*"))
    
    if not model_dirs:
        raise Exception("No models found in the specified directory.")
    
    loaded_models = {}
    for model_dir in model_dirs:
        model_name = os.path.basename(model_dir)
        try:
            model = PipelineModel.load(model_dir)
            loaded_models[model_name] = {
                'model': model,
                'path': model_dir
            }
            logger.info(f"Loaded model {model_name} from {model_dir}")
        except Exception as e:
            logger.error(f"Error loading model from {model_dir}: {str(e)}")
    
    return loaded_models

# Initialize Spark session and models during FastAPI startup
@asynccontextmanager
async def lifespan(app: FastAPI):
    global spark, models
    try:
        logger.info("Initializing Spark session...")
        spark = initialize_spark()
        
        logger.info("Loading models...")
        models = load_models()
        
        if not models:
            raise Exception("No models were loaded successfully")
            
        logger.info(f"Loaded {len(models)} models.")
        
        yield
    except Exception as e:
        logger.error(f"Startup failed: {str(e)}")
        raise
    finally:
        if spark:
            spark.stop()
            logger.info("Spark session stopped")

# FastAPI app setup
app = FastAPI(
    title="Loan Approval Prediction API",
    description="API for predicting loan approval using PySpark ML model",
    version="1.0.0",
    lifespan=lifespan
)

# Helper function to get model information
def get_model_info():
    """Get information about available models"""
    return {
        model_name: {
            'path': info['path']
        }
        for model_name, info in models.items()
    }

# Function to make predictions using a specific model
async def predict_with_model(loan_request: LoanRequest, model_name: str):
    """Make prediction using the specified model"""
    if model_name not in models:
        raise HTTPException(status_code=404, detail=f"Model {model_name} not found")
    
    try:
        # Prepare input data as DataFrame
        data = [(loan_request.Credit_Score, loan_request.LTV, loan_request.loan_amount)]
        df = spark.createDataFrame(data, ["Credit_Score", "LTV", "loan_amount"])
        
        model = models[model_name]['model']
        result = model.transform(df)
        
        prediction_row = result.select("prediction").first()
        
        return PredictionResponse(
            prediction=float(prediction_row["prediction"]),
            prediction_details={
                "model": model_name,
                "model_path": models[model_name]['path']
            }
        )
    except Exception as e:
        logger.error(f"Prediction error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# Define API endpoints
@app.post("/predict/{model_name}", response_model=PredictionResponse)
async def predict_loan_approval(model_name: str, request: LoanRequest):
    """Predict loan approval for a single request using the specified model"""
    return await predict_with_model(request, model_name)

@app.get("/models")
async def list_models():
    """List all available models"""
    return get_model_info()

@app.get("/health-check")
async def health_check():
    """Health check for the service"""
    return {
        "status": "healthy",
        "spark_active": spark is not None,
        "loaded_models": list(models.keys())
    }

@app.get("/")
async def root():
    """Welcome endpoint with available model versions"""
    return {
        "message": "Welcome to the Loan Approval Prediction API!",
        "available_models": get_model_info()
    }

# Run the FastAPI app
if __name__ == "__main__":
    uvicorn.run("api.app:app", host="0.0.0.0", port=8000, reload=True)
