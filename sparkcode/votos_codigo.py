from pyspark.sql import functions as f
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('PySpark_Trabalho_Final').getOrCreate()

print("Reading CSV file from S3")
schema = "sg_uf string, qt_votos_validos int, qt_votos_brancos int, dt_carga string"
votos = spark.read.csv("s3://jvmg-puc-002112264889/votos/", header=True, schema=schema, sep=";", encoding="latin1")

print("Writing dataset as a parquet table")
votos.write.format("parquet").mode("overwrite").save("s3://jvmg-puc-002112264889/parquet/")

print("Votos")
parquet_votos = spark.read.parquet("s3://jvmg-puc-002112264889/parquet/")
ind = parquet_votos.select('sg_uf','qt_votos_validos','qt_votos_brancos')
ind.show()
print(ind)

print("Soma dos Votos")
soma = ind.withColumn('soma_votos', f.expr("qt_votos_validos + qt_votos_brancos"))
soma.show()
print(soma)

print("Subtracao dos Votos")
subtracao = ind.withColumn('subtracao_votos', f.expr("qt_votos_validos - qt_votos_brancos"))
subtracao.show()
print(subtracao)

print("Tabela Final")
tabela_final = ind.withColumn('soma_votos', f.expr("qt_votos_validos + qt_votos_brancos"))
tabela_final = tabela_final.withColumn('subtracao_votos', f.expr("qt_votos_validos - qt_votos_brancos"))
tabela_final.show()
print(tabela_final)
tabela_final.write.mode("overwrite").save("s3://jvmg-puc-002112264889/resultado_final/")
