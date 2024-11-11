from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType, FloatType, DoubleType

# Membuat sesi Spark dengan konektor Kafka
spark = SparkSession.builder \
    .appName("LoanDataProcessor") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
    .getOrCreate()

# Mengonfigurasi Kafka stream
loan_data = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "streaming-data") \
    .load()

# Mendefinisikan skema data loan sesuai dengan struktur kolom dalam CSV
schema = StructType() \
    .add("ID", StringType()) \
    .add("year", IntegerType()) \
    .add("loan_limit", FloatType()) \
    .add("Gender", StringType()) \
    .add("approv_in_adv", StringType()) \
    .add("loan_type", StringType()) \
    .add("loan_purpose", StringType()) \
    .add("Credit_Worthiness", StringType()) \
    .add("open_credit", FloatType()) \
    .add("business_or_commercial", StringType()) \
    .add("loan_amount", FloatType()) \
    .add("rate_of_interest", FloatType()) \
    .add("Interest_rate_spread", FloatType()) \
    .add("Upfront_charges", FloatType()) \
    .add("term", IntegerType()) \
    .add("Neg_ammortization", StringType()) \
    .add("interest_only", StringType()) \
    .add("lump_sum_payment", StringType()) \
    .add("property_value", FloatType()) \
    .add("construction_type", StringType()) \
    .add("occupancy_type", StringType()) \
    .add("Secured_by", StringType()) \
    .add("total_units", IntegerType()) \
    .add("income", FloatType()) \
    .add("credit_type", StringType()) \
    .add("Credit_Score", IntegerType()) \
    .add("co-applicant_credit_type", StringType()) \
    .add("age", IntegerType()) \
    .add("submission_of_application", StringType()) \
    .add("LTV", FloatType()) \
    .add("Region", StringType()) \
    .add("Security_Type", StringType()) \
    .add("Status", StringType()) \
    .add("dtir1", FloatType())

# Mengonversi nilai dari Kafka (format biner) ke JSON dan parsing sesuai skema
loan_df = loan_data \
    .selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

# Menyimpan data ke CSV dalam batch
# Konfigurasi trigger untuk menyimpan setiap sejumlah waktu (misalnya setiap 10 detik)
query = loan_df \
    .writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "output\loan_batch") \
    .option("checkpointLocation", "output\checkpoint") \
    .trigger(processingTime="10 seconds") \
    .start()

query.awaitTermination()
