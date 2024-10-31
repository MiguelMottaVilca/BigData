from pyspark.sql import SparkSession

# Iniciar sesión de Spark
spark = SparkSession.builder.appName("TextSearch").getOrCreate()

# Cargar archivo desde HDFS o S3
# file = spark.read.text("s3://ruta_a_tu_archivo").rdd.map(lambda r: r[0])
file = spark.read.text("hdfs://ip-172-31-92-76.ec2.internal:8020/user/hadoop/log_data.txt").rdd.map(lambda r: r[0])

# Filtrar líneas que contienen "ERROR"
errors = file.filter(lambda line: "ERROR" in line)

# Mapear cada línea con error a un valor de 1 y contar
count = errors.map(lambda line: 1).reduce(lambda x, y: x + y)

print(f"Número de líneas con errores: {count}")

# Cerrar sesión de Spark
spark.stop()
