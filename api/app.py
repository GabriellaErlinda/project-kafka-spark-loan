from flask import Flask, request, jsonify
from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os

# Initialize Flask app and Spark session
app = Flask(__name__)
spark = SparkSession.builder.appName("LoanDefaultPredictionAPI").getOrCreate()

# Load models from saved directories
model_paths = {
    'model_1': '../spark/models/model_1',
    'model_2': '../spark/models/model_2',
    'model_3': '../spark/models/model_3'
}

models = {name: PipelineModel.load(path) for name, path in model_paths.items()}

# Endpoint for making predictions
@app.route('/predict', methods=['POST'])
def predict():
    try:
        # Get JSON data from POST request
        data = request.get_json()

        # Extract relevant features from the JSON payload
        credit_score = data.get('Credit_Score')
        ltv = data.get('LTV')
        loan_amount = data.get('loan_amount')

        # Ensure required fields are present
        if None in [credit_score, ltv, loan_amount]:
            return jsonify({"error": "Missing input data, required fields: Credit_Score, LTV, loan_amount"}), 400

        # Create a DataFrame for prediction
        input_data = spark.createDataFrame([(credit_score, ltv, loan_amount)], ["Credit_Score", "LTV", "loan_amount"])

        # Prepare features vector using the same assembler as during model training
        assembler = VectorAssembler(inputCols=["Credit_Score", "LTV", "loan_amount"], outputCol="features")
        input_data = assembler.transform(input_data)

        # Choose the model to use for prediction (here we use model_1 as example, can be adjusted)
        model = models['model_1']  # Can dynamically choose based on user input if needed

        # Make prediction
        prediction = model.transform(input_data)
        prediction_result = prediction.select("prediction").collect()[0]["prediction"]

        # Return prediction result as JSON response
        return jsonify({"prediction": prediction_result}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
