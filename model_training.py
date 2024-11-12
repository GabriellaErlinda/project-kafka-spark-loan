from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
import os
import syspath

# Inisialisasi Spark Session
spark = SparkSession.builder.appName("Model Training").getOrCreate()

# Baca data batch CSV
data1 = spark.read.csv(r"D:\SEMS-5\BIG DATA\Project2\project-kafka-spark-loan\output\loan_batch\batch_1_20241112150904.csv", header=True, inferSchema=True)
data2 = spark.read.csv(r"D:\SEMS-5\BIG DATA\Project2\project-kafka-spark-loan\output\loan_batch\batch_2_20241112150929.csv", header=True, inferSchema=True)
data3 = spark.read.csv(r"D:\SEMS-5\BIG DATA\Project2\project-kafka-spark-loan\output\loan_batch\batch_3_20241112150953.csv", header=True, inferSchema=True)

# Kolom fitur dan kolom label
feature_columns = ["loan_amount", "rate_of_interest", "Upfront_charges", "term", "property_value", "income", "Credit_Score", "dtir1"]
label_column = "Status"

# Inisialisasi assembler untuk menggabungkan kolom fitur
assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")

# Gabungkan data sesuai dengan model yang akan dilatih
datasets = [
    ("Model1", data1),
    ("Model2", data2),
    ("Model3", data3),
]

# Buat directory untuk menyimpan model
output_dir = "models"
os.makedirs(output_dir, exist_ok=True)

# Inisialisasi evaluator untuk mengukur akurasi
evaluator = MulticlassClassificationEvaluator(labelCol=label_column, predictionCol="prediction", metricName="accuracy")

# Training dan evaluasi untuk setiap model
for model_name, dataset in datasets:
    # Preprocessing pipeline
    assembled_data = assembler.transform(dataset).select("features", label_column)
    
    # Split data menjadi training dan testing
    train_data, test_data = assembled_data.randomSplit([0.8, 0.2], seed=42)
    
    # Inisialisasi RandomForestClassifier
    rf_classifier = RandomForestClassifier(labelCol=label_column, featuresCol="features")
    
    # Train model
    model = rf_classifier.fit(train_data)
    
    # Evaluasi model
    predictions = model.transform(test_data)
    accuracy = evaluator.evaluate(predictions)
    print(f"{model_name} Accuracy: {accuracy}")
    
    # Simpan model
    model_path = os.path.join(output_dir, model_name)
    model.write().overwrite().save(model_path)
    print(f"{model_name} saved at: {model_path}")

# Stop Spark session
spark.stop()
