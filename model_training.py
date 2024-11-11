import os
from pyspark.sql import SparkSession
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline

# Inisialisasi sesi Spark
spark = SparkSession.builder.appName("LoanModelTrainer").getOrCreate()

# Menyiapkan jalur data batch output dari consumer
data_dir = "output/loan_batch"
models_dir = "models"
os.makedirs(models_dir, exist_ok=True)

# Fungsi untuk memuat data batch
def load_data_batches():
    batch_files = [os.path.join(data_dir, f) for f in os.listdir(data_dir) if f.endswith(".csv")]
    dataframes = [spark.read.csv(batch_file, header=True, inferSchema=True) for batch_file in batch_files]
    return dataframes

# Fungsi untuk memeriksa dan membersihkan nama kolom
def clean_column_names(df):
    # Membersihkan nama kolom dengan menghapus spasi atau karakter tak terlihat
    clean_columns = [col.strip() for col in df.columns]
    return df.toDF(*clean_columns)

# Fungsi untuk memeriksa kolom yang tersedia dalam data
def check_columns(df):
    print("Available Columns:", df.columns)
    df.show(5)  # Tampilkan 5 baris pertama untuk memastikan data yang ada

# Mendefinisikan kolom fitur dan kolom label
feature_columns = ["loan_amount", "rate_of_interest", "Upfront_charges", "term", "property_value", "income", "Credit_Score", "dtir1"]
label_column = "Status"  # Kolom target klasifikasi, sesuaikan dengan kolom target dalam dataset

# Fungsi untuk melakukan pelatihan model
def train_and_save_models():
    data_batches = load_data_batches()

    # Periksa dan bersihkan nama kolom di setiap batch data
    clean_batches = [clean_column_names(batch) for batch in data_batches]
    
    # Periksa kolom di setiap batch
    for i, batch in enumerate(clean_batches):
        print(f"Batch {i+1} - Kolom Data: {batch.columns}")
        check_columns(batch)  # Tampilkan kolom dan beberapa baris pertama

    # Skema model: 1/3, 2/3, dan semua data
    batch1 = clean_batches[0]
    batch2 = clean_batches[1] if len(clean_batches) > 1 else None
    batch3 = clean_batches[2] if len(clean_batches) > 2 else None

    # Pipeline untuk klasifikasi
    assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
    classifier = RandomForestClassifier(featuresCol="features", labelCol=label_column)
    pipeline = Pipeline(stages=[assembler, classifier])

    # Model 1: 1/3 data pertama
    model1 = pipeline.fit(batch1)
    model1.write().overwrite().save(os.path.join(models_dir, "model1"))

    # Model 2: 1/3 pertama + 1/3 kedua
    if batch2:
        batch2_data = batch1.union(batch2)
        model2 = pipeline.fit(batch2_data)
        model2.write().overwrite().save(os.path.join(models_dir, "model2"))

    # Model 3: Semua data (1/3 pertama + 1/3 kedua + 1/3 ketiga)
    if batch3:
        all_data = batch1.union(batch2).union(batch3) if batch2 else batch1
        model3 = pipeline.fit(all_data)
        model3.write().overwrite().save(os.path.join(models_dir, "model3"))

train_and_save_models()
spark.stop()
