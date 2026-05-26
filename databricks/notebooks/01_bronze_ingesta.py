# Databricks notebook source
# Databricks notebook source
 
# MAGIC %md
# MAGIC # 🥉 Capa Bronze — Ingesta de Datos NYC Taxi
# MAGIC
# MAGIC ## Descripción
# MAGIC Este notebook implementa la **capa Bronze** de la Arquitectura Medallion para el pipeline
# MAGIC de Business Intelligence del dataset NYC Yellow Taxi (2009-2019).
# MAGIC
# MAGIC ## Responsabilidades de Bronze
# MAGIC - Ingestar los datos **tal como vienen de la fuente**, sin transformaciones de negocio
# MAGIC - Preservar todos los registros, incluyendo datos corruptos o inconsistentes
# MAGIC - Agregar metadatos de auditoría: timestamp de ingesta, fuente y versión de esquema
# MAGIC - Almacenar en formato **Delta Lake** dentro de Unity Catalog
# MAGIC
# MAGIC ## Dataset
# MAGIC - **Fuente:** `/databricks-datasets/nyctaxi/tripdata/yellow/`
# MAGIC - **Período:** 2009 - 2019
# MAGIC - **Total registros:** ~1.6 mil millones
# MAGIC - **Formato original:** CSV comprimido (.csv.gz)
# MAGIC
# MAGIC ## Tablas generadas
# MAGIC | Tabla | Período | Esquema |
# MAGIC |---|---|---|
# MAGIC | `proyecto_bi.bronze.nyc_taxi_v1` | 2009-2015 | GPS (latitud/longitud) |
# MAGIC | `proyecto_bi.bronze.nyc_taxi_v2` | 2016-2019 | Location IDs (PULocationID/DOLocationID) |
# MAGIC
# MAGIC ## Nota sobre los esquemas
# MAGIC En 2016, NYC cambió el formato del dataset: eliminó las coordenadas GPS y las reemplazó
# MAGIC por IDs de zona de taxi. Por eso se manejan dos tablas Bronze separadas que se unifican en Silver.
# MAGIC
# MAGIC ## Autor
# MAGIC Pipeline BI — Bases de Datos Avanzada 2026-I
# MAGIC Universidad Popular del Cesar
 
# COMMAND ----------
 
# MAGIC %md
# MAGIC ## 0. Exploración inicial del dataset
# MAGIC Verificamos que el dataset está disponible en el DBFS de Databricks y listamos los archivos disponibles.
 
# Verificar que el dataset está disponible
display(dbutils.fs.ls("/databricks-datasets/nyctaxi/tripdata/yellow/"))
 


# COMMAND ----------

# COMMAND ----------
 
# MAGIC %md
# MAGIC ## 1. Exploración de esquemas
# MAGIC El dataset tiene dos esquemas diferentes según el período:
# MAGIC - **v1 (2009-2015):** incluye coordenadas GPS (pickup_longitude, pickup_latitude, etc.)
# MAGIC - **v2 (2016-2019):** incluye IDs de zona (PULocationID, DOLocationID)
# MAGIC
# MAGIC Exploramos ambos para entender las diferencias antes de la ingesta.
 
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

# COMMAND ----------
 
# MAGIC %md
# MAGIC ## 2. Configuración de Unity Catalog
# MAGIC Creamos la jerarquía de catálogo/esquema que organiza todas las capas del pipeline:
# MAGIC - **proyecto_bi** → Catálogo principal del proyecto
# MAGIC   - **bronze** → Datos crudos sin transformar
# MAGIC   - **silver** → Datos limpios y normalizados
# MAGIC   - **gold** → Modelos analíticos (Star Schema)
 
# Crear catálogo y esquemas si no existen
spark.sql("CREATE CATALOG IF NOT EXISTS proyecto_bi")
spark.sql("CREATE SCHEMA IF NOT EXISTS proyecto_bi.bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS proyecto_bi.silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS proyecto_bi.gold")
 
print("✅ Catálogos y esquemas creados correctamente")
print("   - proyecto_bi.bronze")
print("   - proyecto_bi.silver")
print("   - proyecto_bi.gold")

# COMMAND ----------

# COMMAND ----------
 
# MAGIC %md
# MAGIC ## 3. Ingesta Bronze v1 — Datos 2009-2015 (Esquema GPS)
# MAGIC
# MAGIC ### Decisión de diseño
# MAGIC Se define el esquema explícitamente en lugar de usar `inferSchema=True` porque el dataset
# MAGIC contiene registros corruptos que causan conflictos de tipos al escribir en Delta Lake.
# MAGIC Todos los campos se leen como **String** para preservar los datos tal como vienen de la fuente.
# MAGIC El casteo a tipos correctos se realiza en la capa Silver con dbt.
# MAGIC
# MAGIC ### Metadatos agregados
# MAGIC - `ingestion_timestamp`: momento exacto de la ingesta
# MAGIC - `source`: identificador de la fuente de datos
# MAGIC - `layer`: capa de la arquitectura medallion
# MAGIC - `schema_version`: versión del esquema para trazabilidad
 
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import current_timestamp, lit
 
# Esquema explícito v1 (2009-2015) — coordenadas GPS
# Todos los campos como String para preservar datos crudos incluyendo valores corruptos
schema_v1 = StructType([
    StructField("vendor_id",          StringType(), True),
    StructField("pickup_datetime",    StringType(), True),
    StructField("dropoff_datetime",   StringType(), True),
    StructField("passenger_count",    StringType(), True),
    StructField("trip_distance",      StringType(), True),
    StructField("pickup_longitude",   StringType(), True),
    StructField("pickup_latitude",    StringType(), True),
    StructField("rate_code",          StringType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("dropoff_longitude",  StringType(), True),
    StructField("dropoff_latitude",   StringType(), True),
    StructField("payment_type",       StringType(), True),
    StructField("fare_amount",        StringType(), True),
    StructField("surcharge",          StringType(), True),
    StructField("mta_tax",            StringType(), True),
    StructField("tip_amount",         StringType(), True),
    StructField("tolls_amount",       StringType(), True),
    StructField("total_amount",       StringType(), True)
])
 
print("📥 Leyendo datos 2009-2015 con esquema explícito...")
 
# Leer archivos 2009
df_2009 = spark.read \
    .option("header", "true") \
    .schema(schema_v1) \
    .csv("/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2009-*.csv.gz")
 
# Leer archivos 2010-2015
df_2010_2015 = spark.read \
    .option("header", "true") \
    .schema(schema_v1) \
    .csv("/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_201[0-5]-*.csv.gz")
 
# Unir ambos períodos — mismo esquema, se pueden combinar directamente
df_old_all = df_2009.union(df_2010_2015)
 
# Agregar metadatos de auditoría Bronze
df_bronze_old = df_old_all \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source",              lit("nyctaxi_yellow_2009_2015")) \
    .withColumn("layer",               lit("bronze")) \
    .withColumn("schema_version",      lit("v1_gps"))
 
print(f"📊 Total registros 2009-2015: {df_bronze_old.count():,}")
 
# Guardar como tabla Delta en Unity Catalog
print("💾 Guardando en proyecto_bi.bronze.nyc_taxi_v1...")
df_bronze_old.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("proyecto_bi.bronze.nyc_taxi_v1")
 
print("✅ Bronze v1 (2009-2015) completado exitosamente")

# COMMAND ----------

# COMMAND ----------
 
# MAGIC %md
# MAGIC ## 4. Verificación Bronze v1
# MAGIC Confirmamos que los datos se guardaron correctamente en Unity Catalog.
 
df_verify_v1 = spark.table("proyecto_bi.bronze.nyc_taxi_v1")
print(f"✅ Registros verificados en Bronze v1: {df_verify_v1.count():,}")
print(f"   Columnas: {len(df_verify_v1.columns)}")
display(df_verify_v1.limit(5))

# COMMAND ----------

# COMMAND ----------
 
# MAGIC %md
# MAGIC ## 5. Ingesta Bronze v2 — Datos 2016-2019 (Esquema Location IDs)
# MAGIC
# MAGIC ### Diferencias con v1
# MAGIC A partir de julio 2016, el TLC de NYC reemplazó las coordenadas GPS por IDs de zona
# MAGIC de taxi (`PULocationID`, `DOLocationID`). Esto requiere un esquema separado y una
# MAGIC tabla Bronze independiente para mantener la integridad de los datos crudos.
# MAGIC
# MAGIC ### Columnas nuevas en v2
# MAGIC - `RatecodeID`: reemplaza a `rate_code`
# MAGIC - `PULocationID`: ID de zona de recogida (reemplaza latitud/longitud)
# MAGIC - `DOLocationID`: ID de zona de destino (reemplaza latitud/longitud)
# MAGIC - `extra`: reemplaza a `surcharge`
# MAGIC - `improvement_surcharge`: cargo adicional por mejoras
# MAGIC - `congestion_surcharge`: cargo por congestión (aplicado desde 2019)
 
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import current_timestamp, lit
 
# Esquema explícito v2 (2016-2019) — Location IDs
schema_v2 = StructType([
    StructField("VendorID",              StringType(), True),
    StructField("tpep_pickup_datetime",  StringType(), True),
    StructField("tpep_dropoff_datetime", StringType(), True),
    StructField("passenger_count",       StringType(), True),
    StructField("trip_distance",         StringType(), True),
    StructField("RatecodeID",            StringType(), True),
    StructField("store_and_fwd_flag",    StringType(), True),
    StructField("PULocationID",          StringType(), True),
    StructField("DOLocationID",          StringType(), True),
    StructField("payment_type",          StringType(), True),
    StructField("fare_amount",           StringType(), True),
    StructField("extra",                 StringType(), True),
    StructField("mta_tax",               StringType(), True),
    StructField("tip_amount",            StringType(), True),
    StructField("tolls_amount",          StringType(), True),
    StructField("improvement_surcharge", StringType(), True),
    StructField("total_amount",          StringType(), True),
    StructField("congestion_surcharge",  StringType(), True)
])
 
print("📥 Leyendo datos 2016-2019 con esquema explícito...")
 
df_new_all = spark.read \
    .option("header", "true") \
    .schema(schema_v2) \
    .csv("/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_201[6-9]-*.csv.gz")
 
# Agregar metadatos de auditoría Bronze
df_bronze_new = df_new_all \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source",              lit("nyctaxi_yellow_2016_2019")) \
    .withColumn("layer",               lit("bronze")) \
    .withColumn("schema_version",      lit("v2_locationid"))
 
print(f"📊 Total registros 2016-2019: {df_bronze_new.count():,}")
 
# Guardar como tabla Delta en Unity Catalog
print("💾 Guardando en proyecto_bi.bronze.nyc_taxi_v2...")
df_bronze_new.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("proyecto_bi.bronze.nyc_taxi_v2")
 
print("✅ Bronze v2 (2016-2019) completado exitosamente")

# COMMAND ----------

# COMMAND ----------
 
# MAGIC %md
# MAGIC ## 6. Verificación Bronze v2
# MAGIC Confirmamos que los datos se guardaron correctamente en Unity Catalog.
 
df_verify_v2 = spark.table("proyecto_bi.bronze.nyc_taxi_v2")
print(f"✅ Registros verificados en Bronze v2: {df_verify_v2.count():,}")
print(f"   Columnas: {len(df_verify_v2.columns)}")
display(df_verify_v2.limit(5))

# COMMAND ----------

# COMMAND ----------
 
# MAGIC %md
# MAGIC ## 7. Resumen final de la capa Bronze
# MAGIC
# MAGIC | Tabla | Período | Registros | Esquema |
# MAGIC |---|---|---|---|
# MAGIC | `proyecto_bi.bronze.nyc_taxi_v1` | 2009-2015 | ~1,179,745,849 | GPS coords |
# MAGIC | `proyecto_bi.bronze.nyc_taxi_v2` | 2016-2019 | ~431,865,186 | Location IDs |
# MAGIC | **Total** | **2009-2019** | **~1,611,611,035** | |
# MAGIC
# MAGIC ### Próximo paso
# MAGIC Los datos están listos para ser consumidos por la **capa Silver** (`02_silver_validacion.py`),
# MAGIC donde se limpiarán, normalizarán y unificarán en un esquema común usando dbt.
 
print("=" * 60)
print("📊 RESUMEN CAPA BRONZE")
print("=" * 60)
 
v1_count = spark.table("proyecto_bi.bronze.nyc_taxi_v1").count()
v2_count = spark.table("proyecto_bi.bronze.nyc_taxi_v2").count()
 
print(f"  Bronze v1 (2009-2015): {v1_count:,} registros")
print(f"  Bronze v2 (2016-2019): {v2_count:,} registros")
print(f"  Total:                 {v1_count + v2_count:,} registros")
print("=" * 60)
print("✅ Capa Bronze completada. Siguiente: Silver con dbt")

# COMMAND ----------

# MAGIC %md
# MAGIC # Documentación Técnica — Capa Bronze
# MAGIC ## Pipeline de Business Intelligence | NYC Yellow Taxi
# MAGIC ### Universidad Popular del Cesar — Bases de Datos Avanzada 2026-I
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 1. ¿Qué es la Capa Bronze?
# MAGIC
# MAGIC La capa Bronze es el primer nivel de la **Arquitectura Medallion**, un patrón de diseño estándar
# MAGIC en la industria de ingeniería de datos que organiza el almacenamiento en tres capas progresivas:
# MAGIC Bronze (crudo), Silver (limpio) y Gold (analítico).
# MAGIC
# MAGIC En esta capa, los datos se ingestan **tal como vienen de la fuente original**, sin aplicar ninguna
# MAGIC transformación de negocio. Esto incluye preservar registros corruptos, valores nulos, formatos
# MAGIC inconsistentes y cualquier anomalía presente en los datos originales. La razón de este principio
# MAGIC es garantizar que siempre exista una fuente de verdad histórica e inmutable a la que se pueda
# MAGIC volver en caso de errores en las capas superiores.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 2. Tecnologías Utilizadas
# MAGIC
# MAGIC ### 2.1 Apache Spark (PySpark)
# MAGIC
# MAGIC Apache Spark es el motor de procesamiento distribuido más utilizado en la industria para el
# MAGIC manejo de grandes volúmenes de datos. Fue creado en la Universidad de Berkeley y actualmente
# MAGIC es mantenido por la Apache Software Foundation. Su principal ventaja frente a tecnologías
# MAGIC anteriores como Hadoop MapReduce es que realiza el procesamiento en memoria (RAM) en lugar
# MAGIC de disco, lo que lo hace entre 10 y 100 veces más rápido para cargas de trabajo analíticas.
# MAGIC
# MAGIC En este proyecto se utilizó **PySpark**, que es la API oficial de Python para Apache Spark.
# MAGIC PySpark permite escribir código en Python que Spark traduce y ejecuta de forma distribuida
# MAGIC sobre múltiples nodos de cómputo. Esto significa que el mismo código que se escribe para
# MAGIC procesar 1,000 registros funciona sin modificaciones para procesar 1,600 millones de registros,
# MAGIC lo cual es exactamente el caso de este proyecto.
# MAGIC
# MAGIC Spark se utilizó en la capa Bronze para las siguientes operaciones:
# MAGIC
# MAGIC - **Lectura de archivos CSV comprimidos** (.csv.gz) directamente desde el sistema de archivos
# MAGIC   de Databricks sin necesidad de descomprimirlos manualmente.
# MAGIC - **Definición de esquemas explícitos** mediante `StructType` y `StructField`, que permiten
# MAGIC   controlar exactamente cómo Spark interpreta cada columna del dataset.
# MAGIC - **Operaciones de transformación** como `withColumn` para agregar columnas de metadatos,
# MAGIC   y `union` para combinar DataFrames con el mismo esquema.
# MAGIC - **Escritura en formato Delta Lake** mediante el método `saveAsTable`.
# MAGIC
# MAGIC ### 2.2 Databricks
# MAGIC
# MAGIC Databricks es una plataforma de datos en la nube fundada por los creadores originales de
# MAGIC Apache Spark. Provee un entorno unificado que combina almacenamiento, procesamiento y
# MAGIC gobernanza de datos en un solo lugar, eliminando la necesidad de configurar y mantener
# MAGIC infraestructura propia.
# MAGIC
# MAGIC Para este proyecto se utilizó la versión **Free Edition** de Databricks, que incluye acceso
# MAGIC a **Serverless Compute**, una modalidad de cómputo donde Databricks gestiona automáticamente
# MAGIC los recursos de procesamiento sin que el usuario necesite configurar clusters manualmente.
# MAGIC Esto permitió ejecutar código PySpark sobre los 1,600 millones de registros del dataset
# MAGIC NYC Taxi sin ninguna configuración de infraestructura adicional.
# MAGIC
# MAGIC Databricks también provee acceso nativo al dataset NYC Taxi a través de su sistema de
# MAGIC datasets de muestra, disponible en la ruta `/databricks-datasets/nyctaxi/`, lo que eliminó
# MAGIC la necesidad de descargar o transferir los datos desde fuentes externas.
# MAGIC
# MAGIC ### 2.3 Delta Lake
# MAGIC
# MAGIC Delta Lake es un formato de almacenamiento de código abierto desarrollado por Databricks
# MAGIC que agrega una capa transaccional sobre archivos Parquet. No es una base de datos separada
# MAGIC ni un sistema externo: físicamente son archivos `.parquet` almacenados en el sistema de
# MAGIC archivos, pero con un registro de transacciones (el `_delta_log`) que les otorga propiedades
# MAGIC que normalmente solo tienen las bases de datos relacionales.
# MAGIC
# MAGIC Las propiedades que Delta Lake agrega a los archivos Parquet son:
# MAGIC
# MAGIC - **ACID Transactions:** garantiza que las escrituras son atómicas, es decir, o se completan
# MAGIC   totalmente o no ocurren, evitando estados intermedios corruptos.
# MAGIC - **Time Travel:** permite consultar versiones anteriores de los datos usando la sintaxis
# MAGIC   `VERSION AS OF` o `TIMESTAMP AS OF`, lo que facilita la auditoría y la recuperación ante errores.
# MAGIC - **Schema Enforcement:** rechaza escrituras que no cumplan con el esquema definido para la tabla,
# MAGIC   previniendo corrupción silenciosa de datos.
# MAGIC - **Optimistic Concurrency Control:** permite que múltiples escrituras concurrentes se resuelvan
# MAGIC   sin bloqueos, usando un mecanismo similar al de los sistemas de control de versiones como Git.
# MAGIC
# MAGIC En la capa Bronze, Delta Lake cumple el rol de repositorio de datos crudos inmutables.
# MAGIC Las tablas Delta generadas (`nyc_taxi_v1` y `nyc_taxi_v2`) sirven como fuente de verdad
# MAGIC histórica que la capa Silver consume para aplicar transformaciones.
# MAGIC
# MAGIC ### 2.4 Unity Catalog
# MAGIC
# MAGIC Unity Catalog es el sistema de gobernanza de metadatos de Databricks. Funciona como un
# MAGIC catálogo centralizado que organiza y registra todas las tablas, vistas y volúmenes de datos
# MAGIC existentes en el workspace, controlando quién puede acceder a qué datos y bajo qué condiciones.
# MAGIC
# MAGIC Unity Catalog organiza los objetos de datos en una jerarquía de tres niveles:
# MAGIC
# MAGIC - **Catálogo:** el nivel más alto, representa un proyecto o dominio de datos. En este proyecto
# MAGIC   se creó el catálogo `proyecto_bi`.
# MAGIC - **Esquema:** agrupa tablas relacionadas dentro de un catálogo. Se crearon tres esquemas:
# MAGIC   `bronze`, `silver` y `gold`, correspondiendo a las tres capas de la arquitectura.
# MAGIC - **Tabla:** el objeto de datos individual. Las tablas Bronze son `nyc_taxi_v1` y `nyc_taxi_v2`.
# MAGIC
# MAGIC La referencia completa a una tabla sigue el formato `catalogo.esquema.tabla`, por ejemplo:
# MAGIC `proyecto_bi.bronze.nyc_taxi_v1`.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 3. Dataset: NYC Yellow Taxi
# MAGIC
# MAGIC El dataset utilizado corresponde a los registros históricos de viajes en taxi amarillo de la
# MAGIC ciudad de Nueva York, publicados por la **NYC Taxi and Limousine Commission (TLC)**. Es uno
# MAGIC de los datasets públicos más utilizados en proyectos de ingeniería de datos y ciencia de datos
# MAGIC debido a su volumen, riqueza de variables y disponibilidad pública.
# MAGIC
# MAGIC El dataset cubre el período de **enero 2009 a diciembre 2019** y contiene información detallada
# MAGIC de cada viaje realizado, incluyendo fechas y horas de recogida y destino, distancia recorrida,
# MAGIC número de pasajeros, tarifas, propinas, métodos de pago y localización geográfica.
# MAGIC
# MAGIC Un detalle importante que se descubrió durante la exploración es que el dataset tiene
# MAGIC **dos esquemas diferentes** según el período:
# MAGIC
# MAGIC **Esquema v1 (2009 - junio 2016):** incluye las coordenadas GPS exactas de recogida y destino
# MAGIC en forma de latitud y longitud. Este esquema tiene 18 columnas.
# MAGIC
# MAGIC **Esquema v2 (julio 2016 - 2019):** el TLC de NYC reemplazó las coordenadas GPS por IDs de
# MAGIC zona de taxi (`PULocationID` y `DOLocationID`), que hacen referencia a 263 zonas geográficas
# MAGIC predefinidas en la ciudad. Este cambio redujo el tamaño de los archivos significativamente
# MAGIC (de ~500MB a ~150MB por mes) y añadió nuevos cargos como `improvement_surcharge` y
# MAGIC `congestion_surcharge`. Este esquema tiene 18 columnas pero diferentes a las del v1.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 4. Decisiones de Diseño
# MAGIC
# MAGIC ### 4.1 Por qué dos tablas Bronze en lugar de una
# MAGIC
# MAGIC La diferencia de esquemas entre los dos períodos hace imposible unificarlos directamente
# MAGIC en una sola tabla sin perder información o forzar transformaciones que no corresponden a Bronze.
# MAGIC Crear dos tablas separadas respeta el principio fundamental de Bronze: preservar los datos
# MAGIC tal como vienen de la fuente. La unificación en un esquema común es responsabilidad de la
# MAGIC capa Silver.
# MAGIC
# MAGIC ### 4.2 Por qué se usó esquema explícito en lugar de inferSchema
# MAGIC
# MAGIC Durante las primeras pruebas de ingesta se utilizó la opción `inferSchema=True`, que permite
# MAGIC a Spark detectar automáticamente el tipo de dato de cada columna. Sin embargo, esto causó
# MAGIC errores al escribir en Delta Lake porque el dataset contiene registros corruptos donde columnas
# MAGIC que deberían ser numéricas contienen valores de texto.
# MAGIC
# MAGIC La solución fue definir el esquema explícitamente usando `StructType` y `StructField`, declarando
# MAGIC todas las columnas como `StringType`. Esto permite que Spark lea todos los valores sin rechazar
# MAGIC ninguno, preservando incluso los datos corruptos en su forma original. El casteo a los tipos
# MAGIC correctos (enteros, decimales, timestamps) se realiza en la capa Silver con dbt, donde también
# MAGIC se documentan y gestionan estas anomalías.
# MAGIC
# MAGIC ### 4.3 Por qué se agregaron metadatos de ingesta
# MAGIC
# MAGIC Cada registro en las tablas Bronze tiene cuatro columnas adicionales que no estaban en los datos
# MAGIC originales: `ingestion_timestamp`, `source`, `layer` y `schema_version`. Estas columnas son
# MAGIC parte de las buenas prácticas de ingeniería de datos porque permiten:
# MAGIC
# MAGIC - Saber exactamente cuándo fue ingestado cada lote de datos.
# MAGIC - Identificar de qué fuente proviene cada registro en caso de múltiples fuentes.
# MAGIC - Confirmar en qué capa de la arquitectura vive el dato.
# MAGIC - Rastrear qué versión de esquema aplica a cada registro, facilitando la evolución del pipeline.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 5. Resultados
# MAGIC
# MAGIC La ejecución del notebook de Bronze produjo los siguientes resultados:
# MAGIC
# MAGIC | Tabla | Período | Registros |
# MAGIC |---|---|---|
# MAGIC | `proyecto_bi.bronze.nyc_taxi_v1` | Enero 2009 — Junio 2016 | 1,179,745,849 |
# MAGIC | `proyecto_bi.bronze.nyc_taxi_v2` | Julio 2016 — Diciembre 2019 | 431,865,186 |
# MAGIC | **Total** | **2009 — 2019** | **1,611,611,035** |
# MAGIC
# MAGIC Los datos se encuentran almacenados en formato Delta Lake dentro de Unity Catalog, listos
# MAGIC para ser consumidos por la capa Silver del pipeline.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 6. Próximo paso: Capa Silver
# MAGIC
# MAGIC Con la capa Bronze completada, el siguiente paso es la **capa Silver**, donde se aplicarán
# MAGIC las siguientes transformaciones usando **dbt (data build tool)**:
# MAGIC
# MAGIC - Casteo de todos los campos String a sus tipos correctos (timestamps, enteros, decimales).
# MAGIC - Eliminación de registros duplicados y nulos.
# MAGIC - Estandarización de valores categóricos (tipos de pago, vendors, etc.).
# MAGIC - Unificación de los dos esquemas Bronze en un esquema Silver común.
# MAGIC - Aplicación de tests de calidad de datos automatizados.
# MAGIC - Documentación del linaje de datos.
