import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

object StreamingConsumer {
  
  def main(args: Array[String]): Unit = {
    
    println("=" * 70)
    println("🚀 SPARK STREAMING CONSUMER DÉMARRÉ")
    println("=" * 70)
    
    // Créer la session Spark
    val spark = SparkSession.builder()
      .appName("Ecommerce Streaming Analytics")
      .master("spark://spark-master:7077")
      .config("spark.sql.streaming.checkpointLocation", "/opt/data/checkpoints")
      .getOrCreate()
    
    import spark.implicits._
    
    // Définir le schéma des données
    val schema = StructType(Array(
      StructField("transaction_id", StringType, true),
      StructField("timestamp", StringType, true),
      StructField("customer_id", StringType, true),
      StructField("customer_name", StringType, true),
      StructField("email", StringType, true),
      StructField("country", StringType, true),
      StructField("city", StringType, true),
      StructField("category", StringType, true),
      StructField("product", StringType, true),
      StructField("quantity", IntegerType, true),
      StructField("unit_price", DoubleType, true),
      StructField("total_amount", DoubleType, true),
      StructField("payment_method", StringType, true),
      StructField("status", StringType, true)
    ))
    
    // Lire depuis Kafka
    val kafkaStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka:9092")
      .option("subscribe", "ecommerce-transactions")
      .option("startingOffsets", "latest")
      .load()
    
    // Parser le JSON
    val transactions = kafkaStream
      .selectExpr("CAST(value AS STRING) as json")
      .select(from_json($"json", schema).as("data"))
      .select("data.*")
    
    // Traitement 1: Statistiques par pays
    val statsByCountry = transactions
      .filter($"status" === "completed")
      .groupBy($"country")
      .agg(
        count("*").as("nb_transactions"),
        sum("total_amount").as("revenue"),
        avg("total_amount").as("avg_order_value")
      )
      .orderBy($"revenue".desc)
    
    // Traitement 2: Top produits
    val topProducts = transactions
      .filter($"status" === "completed")
      .groupBy($"category", $"product")
      .agg(
        count("*").as("nb_sales"),
        sum("quantity").as("total_quantity"),
        sum("total_amount").as("total_revenue")
      )
      .orderBy($"total_revenue".desc)
    
    // Traitement 3: Transactions en échec
    val failedTransactions = transactions
      .filter($"status" === "failed")
      .select($"transaction_id", $"customer_name", $"total_amount", $"country")
    
    println("📊 Démarrage des requêtes streaming...")
    println()
    
    // Output 1: Afficher stats pays dans la console
    val query1 = statsByCountry.writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", false)
      .option("numRows", 10)
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()
    
    // Output 2: Sauvegarder dans HDFS/Data (format Parquet)
    val query2 = transactions
      .filter($"status" === "completed")
      .writeStream
      .outputMode("append")
      .format("parquet")
      .option("path", "/opt/data/processed/transactions")
      .option("checkpointLocation", "/opt/data/checkpoints/transactions")
      .trigger(Trigger.ProcessingTime("30 seconds"))
      .start()
    
    // Output 3: Top produits dans la console
    val query3 = topProducts.writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", false)
      .option("numRows", 5)
      .trigger(Trigger.ProcessingTime("15 seconds"))
      .start()
    
    println("✅ Toutes les requêtes streaming sont actives!")
    println("📈 Les données sont traitées et sauvegardées...")
    println()
    println("Pour arrêter: Ctrl+C")
    println("=" * 70)
    
    // Attendre la fin
    query1.awaitTermination()
  }
}