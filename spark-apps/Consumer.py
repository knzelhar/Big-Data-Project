from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

print("=" * 70)
print("🚀 SPARK STREAMING CONSUMER DÉMARRÉ (PySpark)")
print("=" * 70)

# Créer la session Spark
spark = SparkSession.builder \
    .appName("Ecommerce Streaming Analytics") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.streaming.checkpointLocation", "/opt/data/checkpoints") \
    .getOrCreate()

# Définir le schéma
schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("customer_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("country", StringType(), True),
    StructField("city", StringType(), True),
    StructField("category", StringType(), True),
    StructField("product", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("payment_method", StringType(), True),
    StructField("status", StringType(), True)
])

# Lire depuis Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "ecommerce-transactions") \
    .option("startingOffsets", "latest") \
    .load()

# Parser JSON
transactions = kafka_df \
    .selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

# Traitement 1: Statistiques par pays
stats_by_country = transactions \
    .filter(col("status") == "completed") \
    .groupBy("country") \
    .agg(
        count("*").alias("nb_transactions"),
        sum("total_amount").alias("revenue"),
        avg("total_amount").alias("avg_order_value")
    ) \
    .orderBy(col("revenue").desc())

# Traitement 2: Top produits
top_products = transactions \
    .filter(col("status") == "completed") \
    .groupBy("category", "product") \
    .agg(
        count("*").alias("nb_sales"),
        sum("quantity").alias("total_quantity"),
        sum("total_amount").alias("total_revenue")
    ) \
    .orderBy(col("total_revenue").desc())

print("📊 Démarrage des requêtes streaming...")
print()

# Output 1: Stats pays dans console
query1 = stats_by_country.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .option("numRows", 10) \
    .trigger(processingTime='10 seconds') \
    .start()

# Output 2: Sauvegarder en Parquet
query2 = transactions \
    .filter(col("status") == "completed") \
    .writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "/opt/data/processed/transactions") \
    .option("checkpointLocation", "/opt/data/checkpoints/transactions") \
    .trigger(processingTime='30 seconds') \
    .start()

# Output 3: Top produits
query3 = top_products.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .option("numRows", 5) \
    .trigger(processingTime='15 seconds') \
    .start()

print("✅ Toutes les requêtes streaming sont actives!")
print("📈 Les données sont traitées et sauvegardées...")
print()
print("Pour arrêter: Ctrl+C")
print("=" * 70)

query1.awaitTermination()
