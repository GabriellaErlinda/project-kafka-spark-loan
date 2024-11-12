from flask import Flask, request, jsonify
from pyspark.ml.classification import LogisticRegressionModel
from pyspark.ml.linalg import Vectors

app = Flask(__name__)

# Load models
model1 = LogisticRegressionModel.load("model1")
model2 = LogisticRegressionModel.load("model2")
model3 = LogisticRegressionModel.load("model3")

@app.route('/recommend', methods=['POST'])
def recommend():
    data = request.json
    features = Vectors.dense([data['feature1'], data['feature2']])
    model_name = data.get('model', 'model1')  # Default to model1 if not specified

    # Pilih model yang sesuai
    if model_name == 'model2':
        model = model2
    elif model_name == 'model3':
        model = model3
    else:
        model = model1

    # Gunakan model untuk klasifikasi
    prediction = model.transform(features)
    predicted_class = prediction.select("prediction").first().prediction
    return jsonify({"predicted_class": predicted_class})

if __name__ == '__main__':
    app.run(port=5000)
