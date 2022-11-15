from pyspark.sql import functions as f
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.1.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

from delta.tables import *

print("Reading CSV file from S3")
schema1 = "sg_uf string, qt_votos_brancos int, dt_carga string"
votos_brancos = spark.read.csv("s3://jvmg-puc-002112264889/votos_brancos/", header=True, schema=schema1, sep=";")
ind_brancos = votos_brancos.select('sg_uf','qt_votos_brancos')
schema2 = "sg_uf string, qt_votos_nom_validos int, dt_carga string"
votos_validos = spark.read.csv("s3://jvmg-puc-002112264889/votos_validos/", header=True, schema=schema2, sep=";")
ind_validos = votos_validos.select('sg_uf','qt_votos_nom_validos')

print("Writing dataset as a parquet table")
ind_brancos.write.format("parquet").mode("overwrite").save("s3://jvmg-puc-002112264889/parquet_brancos/")
ind_validos.write.format("parquet").mode("overwrite").save("s3://jvmg-puc-002112264889/parquet_validos/")

print("Votos Brancos")
parquet_brancos = spark.read.parquet("s3://jvmg-puc-002112264889/parquet_brancos/").show()

print("Votos Válidos")
parquet_validos = spark.read.parquet("s3://jvmg-puc-002112264889/parquet_validos/").show()

print("Tabela Final")
tabela_final = parquet_brancos.merge(parquet_validos, how='inner', on=['sg_uf']).show()
tabela_final.write.mode("overwrite").save("s3://jvmg-puc-002112264889/resultado_final/")