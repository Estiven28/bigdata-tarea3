"""
Análisis en Batch con Apache Spark
Dataset: Accidentalidad Vial Guadalajara de Buga
Autor: TU NOMBRE
Curso: Big Data - UNAD
"""

# ============================================
# SECCIÓN 1 - IMPORTAR LIBRERÍAS
# ============================================

from pyspark.sql import SparkSession, functions as F
import time

# ============================================
# SECCIÓN 2 - CREAR SESIÓN DE SPARK
# ============================================

spark = SparkSession.builder \
    .appName("Accidentes_Buga_Batch") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("\n" + "="*50)
print("ANÁLISIS BATCH - ACCIDENTES BUGA")
print("="*50)

# ============================================
# SECCIÓN 3 - CARGAR DATASET CSV
# ============================================

df = spark.read.csv(
    "buga.csv",
    header=True,
    inferSchema=True
)

print("\nColumnas del dataset:")
print(df.columns)

print("\nPrimeros registros:")
df.show(5)

# ============================================
# SECCIÓN 4 - EXPLORACIÓN DE DATOS
# ============================================

print("\nEsquema del DataFrame:")
df.printSchema()

print("\nEstadísticas básicas:")
df.summary().show()

print("\nTotal de registros:")
print(df.count())

# ============================================
# SECCIÓN 5 - ANÁLISIS (CONSULTAS)
# ============================================

# Validación de columnas antes de usar
if "edad" in df.columns:
    print("\nPersonas con edad mayor a 50:\n")
    df.filter(F.col("edad") > 50).show()

if "sexo" in df.columns:
    print("\nDistribución por sexo:\n")
    df.groupBy("sexo").count().show()

# ============================================
# SECCIÓN 6 - RESUMEN FINAL
# ============================================

print("\n===== RESUMEN FINAL =====")
print(f"Total de columnas: {len(df.columns)}")
print("Análisis realizado correctamente")

print("="*50)

# Mantener Spark activo unos segundos (para UI)
time.sleep(60)

spark.stop()
