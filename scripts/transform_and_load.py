# -*- coding: utf-8 -*-
# Usage: chmod +x transform_and_load.py && spark-submit transform_and_load.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, lit, coalesce, lower, regexp_replace
from pyspark.sql.types import IntegerType, DateType, FloatType
import subprocess
import sys

# Initialize status flags
vuelos_ok = False
detalles_ok = False

# ===============================================
# 0.  SPARK INITIALIZATION
# ===============================================

try:
    spark = SparkSession.builder \
        .appName("AereoIngestPipeline") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .enableHiveSupport() \
        .getOrCreate()
    print("Spark Session initialized successfully.")

except Exception as e:
    print(f"ERROR initializing Spark Session: {e}")

    exit(1)

HDFS_LANDING_PATH = "hdfs://172.17.0.2:9000/home/hadoop/landing/aereo"

# ========================
# 1. HELPER FUNCTIONS
# ========================

def clean_mojibake(column):
    cleaned_col = regexp_replace(column, "   ^c      ", "      ")
    cleaned_col = regexp_replace(cleaned_col, "   ^c      ", "      ")
    cleaned_col = regexp_replace(cleaned_col, "   ^c      ", "      ")
    cleaned_col = regexp_replace(cleaned_col, "   ^c      ", "      ")
    cleaned_col = regexp_replace(cleaned_col, "   ^c      ", "      ")
    cleaned_col = regexp_replace(cleaned_col, "   ^c      ", "      ")
    cleaned_col = regexp_replace(cleaned_col, "   ^c\u0081", "   ^a")
    cleaned_col = regexp_replace(cleaned_col, "   ^c\u0089", "   ^i")
    cleaned_col = regexp_replace(cleaned_col, "   ^c\u008D", "   ^m")
    cleaned_col = regexp_replace(cleaned_col, "   ^c\u0093", "   ^s")
    cleaned_col = regexp_replace(cleaned_col, "   ^c\u009A", "   ^z")
    cleaned_col = regexp_replace(cleaned_col, "   ^c\u0091", "   ^q")
    cleaned_col = regexp_replace(cleaned_col, "   ^b      ", "      ")
    return cleaned_col

# ===============================================
# 2. FLIGHT DATA PROCESSING AND LOADING
# ===============================================
COLUMN_MAPPING = [
    ("Fecha", "fecha"),
    ("Hora UTC", "horautc"),
    ("Clase de Vuelo (todos los vuelos)", "clase_de_vuelo"),
    ("ClasificaciÃ³n Vuelo","clasificacion_de_vuelo"),
    ("Tipo de Movimiento", "tipo_de_movimiento"),
    ("Aeropuerto", "aeropuerto"),
    ("Origen / Destino", "origen_destino"),
    ("Aerolinea Nombre", "aerolinea_nombre"),
    ("Aeronave", "aeronave"),
    ("Pasajeros", "pasajeros")
]

try:
    print("\n--- Starting processing for flight data (2021 and 2022) ---")

    path_2021 = f"{HDFS_LANDING_PATH}/2021-informe-ministerio.csv"
    path_2022 = f"{HDFS_LANDING_PATH}/202206-informe-ministerio.csv"

    df_2021 = spark.read.csv(path_2021, header=True, inferSchema=True, sep=';', encoding= 'ISO-8859-1')
    df_2022 = spark.read.csv(path_2022, header=True, inferSchema=True, sep=';', encoding= 'ISO-8859-1')

    df_union = df_2021.unionByName(df_2022, allowMissingColumns=True)

    df_selected = df_union.select(
        *[col(csv_name).alias(hive_name) for csv_name, hive_name in COLUMN_MAPPING]
    )
  
    df_normalized = df_selected
  
    columns_to_clean = [
        "clase_de_vuelo",
        "clasificacion_de_vuelo",
        "aeropuerto",
        "origen_destino",
        "aerolinea_nombre",
        "aeronave"
    ]
    for col_name in columns_to_clean:
        df_normalized = df_normalized.withColumn(col_name, clean_mojibake(col(col_name)))

    df_domestic = df_normalized.filter(
       lower(col("clasificacion_de_vuelo")) != lit("internacional")
    )

    df_clean = df_domestic.withColumn("fecha", to_date(col("fecha"), "dd/MM/yyyy"))

    df_casted = df_clean.withColumn(
        "pasajeros",
        coalesce(col("pasajeros"), lit(0)).cast("int")
    )

    START_DATE = "2021-01-01"
    END_DATE = "2022-06-30"

    df_filtered = df_casted.filter(
        (col("fecha").isNotNull()) &
        (col("fecha") >= lit(START_DATE).cast('date')) &
        (col("fecha") <= lit(END_DATE).cast('date'))
    )

    print(f"Loading {df_filtered.count()} filtered records into aereo.aeropuerto_tabla...")
    df_filtered.write.mode("overwrite").insertInto("aereo.aeropuerto_tabla")
    print("Flight data load completed successfully.")
    vuelos_ok = True
  
except Exception as e:
    print(f"ERROR loading FLIGHT data: {e}")

# ===============================================
# 3. AIRPORT DETAILS DATA LOAD
# ===============================================
COLUMN_MAPPING_DETAILS = [
    ("local", "aeropuerto"),
    ("oaci", "oac"),
    ("iata", "iata"),
    ("tipo", "tipo"),
    ("denominacion", "denominacion"),
    ("coordenadas", "coordenadas"),
    ("latitud", "latitud"),
    ("longitud", "longitud"),
    ("elev", "elev"),
    ("uom_elev", "uom_elev"),
    ("ref", "ref"),
    ("distancia_ref", "distancia_ref"),
    ("direccion_ref", "direccion_ref"),
    ("condicion", "condicion"),
    ("control", "control"),
    ("region", "region"),
    ("uso", "uso"),
    ("trafico", "trafico"),
    ("sna", "sna"),
    ("concesionado", "concesionado"),
    ("provincia", "provincia")
]
try:
    print("\n--- Starting airport details data load ---")

    path_details = f"{HDFS_LANDING_PATH}/aeropuertos_detalle.csv"

    df_details = spark.read.csv(path_details, header=True, inferSchema=True, sep=';', encoding= 'ISO-8859-1')

    df_normalized_details = df_details

    columns_to_clean_details = [
        "local",
        "tipo",
        "denominacion",
        "coordenadas",
        "elev",
        "uom_elev",
        "ref",
        "condicion",
        "control",
        "region",
        "uso",
        "trafico",
        "sna",
        "concesionado",
        "provincia",
        "coordenadas"
    ]

    for col_name in columns_to_clean_details:
        df_normalized_details = df_normalized_details.withColumn(col_name, clean_mojibake(col(col_name)))

    df_selected = df_normalized_details.select(
        *[col(csv_name).alias(hive_name) for csv_name, hive_name in COLUMN_MAPPING_DETAILS]
    )

    df_final = df_selected \
        .withColumn(
            "elev",
            coalesce(col("elev").cast(FloatType()), lit(0.0))
        ) \
        .withColumn(
            "distancia_ref",
            coalesce(col("distancia_ref"), lit(0)).cast('float')
        )


    print(f"Loading {df_final.count()} records into aereo.aeropuerto_detalles_tabla...")
    df_final.write.mode("overwrite").insertInto("aereo.aeropuerto_detalles_tabla")
    print("Airport Details load completed successfully.")
    detalles_ok = True

except Exception as e:
    print(f"ERROR loading DETAILS data: {e}")

# ===============================================
# 4. FINAL STATUS REPORT
# ===============================================
print("\n" + "="*53)
if vuelos_ok and detalles_ok:
    print("ALL STAGES COMPLETED: Both tables are now live in Hive.")
else:
    print("WARNING: Pipeline finished but some stages might have failed.")
    print(f"Status - Flights: {'OK' if vuelos_ok else 'FAILED'}")
    print(f"Status - Details: {'OK' if detalles_ok else 'FAILED'}")
print("="*53)

spark.stop()
