# Databricks notebook source
# Verificar que el dataset está disponible
display(dbutils.fs.ls("/databricks-datasets/nyctaxi/tripdata/yellow/"))

# COMMAND ----------

# Esquema antiguo (2009-2015) — tiene coordenadas GPS
df_old = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2010-01.csv.gz")

print("=== ESQUEMA ANTIGUO (2009-2015) ===")
df_old.printSchema()

# Esquema nuevo (2016-2019) — tiene location IDs
df_new = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2019-01.csv.gz")

print("=== ESQUEMA NUEVO (2016-2019) ===")
df_new.printSchema()

print(f"\nRegistros 2010-01: {df_old.count()}")
print(f"Registros 2019-01: {df_new.count()}")

# COMMAND ----------

# Primero creamos el catálogo y esquema
spark.sql("CREATE CATALOG IF NOT EXISTS proyecto_bi")
spark.sql("CREATE SCHEMA IF NOT EXISTS proyecto_bi.bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS proyecto_bi.silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS proyecto_bi.gold")

print("✅ Catálogos creados")

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import current_timestamp, lit

# Esquema explícito v1 — evita inferencia problemática
schema_v1 = StructType([
    StructField("vendor_id", StringType(), True),
    StructField("pickup_datetime", StringType(), True),
    StructField("dropoff_datetime", StringType(), True),
    StructField("passenger_count", StringType(), True),
    StructField("trip_distance", StringType(), True),
    StructField("pickup_longitude", StringType(), True),
    StructField("pickup_latitude", StringType(), True),
    StructField("rate_code", StringType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("dropoff_longitude", StringType(), True),
    StructField("dropoff_latitude", StringType(), True),
    StructField("payment_type", StringType(), True),
    StructField("fare_amount", StringType(), True),
    StructField("surcharge", StringType(), True),
    StructField("mta_tax", StringType(), True),
    StructField("tip_amount", StringType(), True),
    StructField("tolls_amount", StringType(), True),
    StructField("total_amount", StringType(), True)
])

print("📥 Releyendo con esquema explícito...")

df_2009 = spark.read \
    .option("header", "true") \
    .schema(schema_v1) \
    .csv("/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2009-*.csv.gz")

df_2010_2015 = spark.read \
    .option("header", "true") \
    .schema(schema_v1) \
    .csv("/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_201[0-5]-*.csv.gz")

df_old_all = df_2009.union(df_2010_2015)

df_bronze_old = df_old_all \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source", lit("nyctaxi_yellow_2009_2015")) \
    .withColumn("layer", lit("bronze")) \
    .withColumn("schema_version", lit("v1_gps"))

print(f"📊 Total registros: {df_bronze_old.count()}")

print("💾 Guardando Bronze v1...")
df_bronze_old.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("proyecto_bi.bronze.nyc_taxi_v1")

print("✅ Bronze v1 completado")

# COMMAND ----------

df_verify = spark.table("proyecto_bi.bronze.nyc_taxi_v1")
print(f"✅ Registros en Bronze v1: {df_verify.count()}")
df_verify.show(5)

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import current_timestamp, lit

# Esquema explícito v2
schema_v2 = StructType([
    StructField("VendorID", StringType(), True),
    StructField("tpep_pickup_datetime", StringType(), True),
    StructField("tpep_dropoff_datetime", StringType(), True),
    StructField("passenger_count", StringType(), True),
    StructField("trip_distance", StringType(), True),
    StructField("RatecodeID", StringType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("PULocationID", StringType(), True),
    StructField("DOLocationID", StringType(), True),
    StructField("payment_type", StringType(), True),
    StructField("fare_amount", StringType(), True),
    StructField("extra", StringType(), True),
    StructField("mta_tax", StringType(), True),
    StructField("tip_amount", StringType(), True),
    StructField("tolls_amount", StringType(), True),
    StructField("improvement_surcharge", StringType(), True),
    StructField("total_amount", StringType(), True),
    StructField("congestion_surcharge", StringType(), True)
])

print("📥 Leyendo 2016-2019...")

df_new_all = spark.read \
    .option("header", "true") \
    .schema(schema_v2) \
    .csv("/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_201[6-9]-*.csv.gz")

df_bronze_new = df_new_all \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source", lit("nyctaxi_yellow_2016_2019")) \
    .withColumn("layer", lit("bronze")) \
    .withColumn("schema_version", lit("v2_locationid"))

print(f"📊 Total registros 2016-2019: {df_bronze_new.count()}")

print("💾 Guardando Bronze v2...")
df_bronze_new.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("proyecto_bi.bronze.nyc_taxi_v2")

print("✅ Bronze v2 completado")