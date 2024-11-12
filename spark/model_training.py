from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
import os
import json

# Initialize Spark session
spark = SparkSession.builder.appName("LoanDefaultModelTraining").getOrCreate()

batch_dir = os.path.abspath('../data/batches')
batch_files = sorted([os.path.join(batch_dir, f) for f in os.listdir(batch_dir) if f.endswith('.json')])

# Function to load and preprocess a single batch
def load_batch(file_path):
    with open(file_path, 'r') as f:
        batch_data = json.load(f)
    return spark.createDataFrame(batch_data)

# Load batches and concatenate as needed for each model
batches = [load_batch(file) for file in batch_files]

# Determine sizes for the model data subsets
num_batches = len(batches)
if num_batches < 3:
    raise ValueError("At least 3 batches are required for this task.")

# Concatenate batches for each model
data_model_1 = batches[0]
data_model_2 = batches[0].union(batches[1])
data_model_3 = batches[0].union(batches[1]).union(batches[2])

# Function to train a model and save it
def train_and_save_model(data, model_name):
    # Convert necessary columns to float type
    data = data.withColumn("Credit_Score", data["Credit_Score"].cast("float")) \
               .withColumn("LTV", data["LTV"].cast("float")) \
               .withColumn("loan_amount", data["loan_amount"].cast("float")) \
               .withColumn("Status", data["Status"].cast("float"))  # Assuming Status is the label for classification

    # Handle missing or invalid values (if any)
    data = data.dropna(subset=["Credit_Score", "LTV", "loan_amount", "Status"])

    # Feature engineering: create a features vector with existing columns
    assembler = VectorAssembler(inputCols=["Credit_Score", "LTV", "loan_amount"], outputCol="features")
    lr = LogisticRegression(featuresCol="features", labelCol="Status")

    pipeline = Pipeline(stages=[assembler, lr])

    # Fit the model
    model = pipeline.fit(data)

    # Save the model
    model.save(f"../spark/models/{model_name}")

# Train models on each subset
os.makedirs('../spark/models', exist_ok=True)
train_and_save_model(data_model_1, "model_1")
train_and_save_model(data_model_2, "model_2")
train_and_save_model(data_model_3, "model_3")

# Stop Spark session
spark.stop()
