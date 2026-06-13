from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType

# Démarrer Spark
spark = SparkSession.builder \
    .appName("SecuStream") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
            "org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("✅ Spark démarré, connexion à Kafka...")

# Schéma des événements NSL-KDD
schema = StructType() \
    .add('duration', StringType()) \
    .add('protocol_type', StringType()) \
    .add('service', StringType()) \
    .add('flag', StringType()) \
    .add('src_bytes', StringType()) \
    .add('dst_bytes', StringType()) \
    .add('num_failed_logins', StringType()) \
    .add('logged_in', StringType()) \
    .add('count', StringType()) \
    .add('label', StringType()) \
    .add('timestamp', DoubleType()) \
    .add('event_id', IntegerType())

# Lire le flux Kafka
raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "security-events") \
    .option("startingOffsets", "latest") \
    .load()

# Parser les messages JSON
events = raw.select(
    from_json(col("value").cast("string"), schema).alias("d")
).select("d.*")

# Détection d'anomalies
alerts = events.withColumn(
    "is_attack",
    when(col("label") != "normal", True).otherwise(False)
).withColumn(
    "attack_type",
    when(col("label") == "normal", "normal")
    .otherwise(col("label"))
)

# -----------------------------------------------
# ÉCRITURE VERS POSTGRESQL + PARQUET
# -----------------------------------------------

def save_to_postgres_and_parquet(df, epoch_id):
    if df.count() == 0:
        return

    # Sélectionner les colonnes
    data = df.select(
        "protocol_type",
        "service",
        "flag",
        "label",
        "is_attack",
        "attack_type",
        "timestamp"
    )

    # 1. Sauvegarder dans PostgreSQL
    data.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/secudb") \
        .option("dbtable", "security_events") \
        .option("user", "admin") \
        .option("password", "secret") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

    print(f"✅ Batch {epoch_id} sauvegardé dans PostgreSQL")

    # 2. Sauvegarder en Parquet
    data.write \
        .mode("append") \
        .parquet(f"/opt/spark-apps/historique/batch_{epoch_id}.parquet")

    print(f"✅ Batch {epoch_id} sauvegardé en Parquet")

# Lancer le streaming
query = alerts.writeStream \
    .outputMode("append") \
    .foreachBatch(save_to_postgres_and_parquet) \
    .trigger(processingTime="10 seconds") \
    .start()

print("✅ Streaming démarré ! En attente d'événements...")
query.awaitTermination()
