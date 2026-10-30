
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("AnalisisAccidentesTransitoColombia") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

ruta = "/home/vboxuser/tarea3/datos/accidentes.csv"

df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "false") \
    .option("encoding", "UTF-8") \
    .csv(ruta)

df = df.toDF(*[
    c.replace("\ufeff", "").strip().upper()
    for c in df.columns
])

columnas = [
    "TIPO_VEHICULO",
    "EDAD_VEHICULO",
    "FECHA_ACCIDENTE",
    "GRAVEDAD_ACCIDENTE",
    "DEPARTAMENTO_ACCIDENTE"
]

faltantes = [c for c in columnas if c not in df.columns]

if faltantes:
    raise ValueError(f"Columnas no encontradas: {faltantes}")

print("\n=== ESTRUCTURA DE LA BASE ===")
df.printSchema()

print("\n=== PRIMEROS CINCO REGISTROS ===")
df.show(5, truncate=False)

print("\n=== TOTAL DE REGISTROS ===")
print(df.count())

print("\n=== VALORES NULOS O VACIOS ===")

df.select([
    F.sum(
        F.when(
            F.col(c).isNull() | (F.trim(F.col(c)) == ""),
            1
        ).otherwise(0)
    ).alias(c)
    for c in columnas
]).show(truncate=False)

def normalizar(columna):
    return F.when(
        F.col(columna).isNull() |
        (F.trim(F.col(columna)) == ""),
        "SIN DATO"
    ).otherwise(
        F.upper(F.trim(F.col(columna)))
    )

datos = df \
    .withColumn(
        "TIPO_LIMPIO", normalizar("TIPO_VEHICULO")
    ) \
    .withColumn(
        "DEPARTAMENTO_LIMPIO",
        normalizar("DEPARTAMENTO_ACCIDENTE")
    ) \
    .withColumn(
        "GRAVEDAD_LIMPIA",
        normalizar("GRAVEDAD_ACCIDENTE")
    ) \
    .withColumn(
        "EDAD_NUMERICA",
        F.regexp_replace(
            F.col("EDAD_VEHICULO"), ",", "."
        ).cast("double")
    ) \
    .withColumn(
        "ANIO",
        F.regexp_extract(
            F.col("FECHA_ACCIDENTE"),
            r"(19\d{2}|20\d{2})", 1
        )
    )

print("\n=== VEHICULOS POR TIPO ===")

por_tipo = datos.groupBy("TIPO_LIMPIO") \
    .count() \
    .orderBy(F.desc("count"))

por_tipo.show(20, truncate=False)

print("\n=== REGISTROS POR DEPARTAMENTO ===")

por_departamento = datos.groupBy(
    "DEPARTAMENTO_LIMPIO"
).count().orderBy(F.desc("count"))

por_departamento.show(20, truncate=False)

print("\n=== GRAVEDAD DE LOS ACCIDENTES ===")

por_gravedad = datos.groupBy(
    "GRAVEDAD_LIMPIA"
).count().orderBy(F.desc("count"))

por_gravedad.show(truncate=False)

print("\n=== ANTIGUEDAD PROMEDIO POR VEHICULO ===")

por_antiguedad = datos.groupBy("TIPO_LIMPIO") \
    .agg(
        F.round(
            F.avg("EDAD_NUMERICA"), 2
        ).alias("EDAD_PROMEDIO")
    ) \
    .orderBy(F.desc("EDAD_PROMEDIO"))

por_antiguedad.show(20, truncate=False)

print("\n=== REGISTROS POR ANIO ===")

por_anio = datos.filter(
    F.col("ANIO") != ""
).groupBy("ANIO").count().orderBy("ANIO")

por_anio.show(30, truncate=False)

salida = "/home/vboxuser/tarea3/resultados/"

resultados = {
    "tipos": por_tipo,
    "departamentos": por_departamento,
    "gravedad": por_gravedad,
    "antiguedad": por_antiguedad,
    "anios": por_anio
}

for nombre, tabla in resultados.items():
    tabla.coalesce(1) \
        .write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(salida + nombre)

print("\nANALISIS COMPLETADO")
print("Resultados guardados en:", salida)

spark.stop()
