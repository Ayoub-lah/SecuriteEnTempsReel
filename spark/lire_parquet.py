from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("LireParquet") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Lire tous les fichiers snappy.parquet récursivement
df = spark.read \
    .option("recursiveFileLookup", "true") \
    .parquet("/opt/spark-apps/historique/")

print("✅ Nombre total de lignes :", df.count())
print("\n📊 Aperçu des données :")
df.show(10)

print("\n🚨 Statistiques des attaques :")
df.groupBy("attack_type") \
  .count() \
  .orderBy("count", ascending=False) \
  .show()
