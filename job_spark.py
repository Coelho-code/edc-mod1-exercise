from pyspark.sql.functions import mean, max, min, col, count
from pyspark.sql import Sparksession

spark = (
    Sparksession.builder.appName("ExerciseSpark")
    .getOrCreate()
)

#Ler os dados do enem 2019
enem = (
    spark
    .read
    .format("csv")
    .option("header","True")
    .option("inferSchema","True")
    .option("delimiter",";")
    .load("s3://lucas-modulo1/MICRODADOS_ENEM_2020.csv")
)

(
    enem
    .write
    .mode("overwrite")
    .format("parquet")
    .partitionBy("year")
    .save("s3://lucas-modulo1/teste.csv")
)