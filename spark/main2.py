import numpy as np
from pyspark.sql import SparkSession
from pyspark.accumulators import AccumulatorParam

# Iniciar sesión de Spark
spark = SparkSession.builder.appName("LogisticRegression").getOrCreate()

# Función para parsear cada línea del archivo a un punto de datos
def parse_point(line):
    values = [float(x) for x in line.split(',')]
    return (np.array(values[:-1]), values[-1])

# Cargar puntos de datos desde un archivo y almacenarlos en caché
# points = spark.read.text("s3://ruta_a_tu_archivo").rdd.map(lambda r: parse_point(r[0])).cache()
points = spark.read.text("hdfs://ip-172-31-92-76.ec2.internal:8020/user/hadoop/logistic_data.txt").rdd.map(lambda r: parse_point(r[0])).cache()

# Inicializar el vector w aleatorio
D = len(points.first()[0])  # Dimensión
w = np.random.rand(D)

# Número de iteraciones
ITERATIONS = 10
learning_rate = 0.01

# Ejecutar iteraciones de gradiente descendente
for i in range(ITERATIONS):
    gradient = np.zeros(D)
    
    def gradient_sum(point):
        x, y = point
        dot_product = np.dot(w, x)
        sigmoid = 1 / (1 + np.exp(-y * dot_product))
        return x * (sigmoid - 1) * y
    
    # Calcular gradiente usando map-reduce
    gradient = points.map(lambda p: gradient_sum(p)).reduce(lambda x, y: x + y)
    
    # Actualizar el vector w
    w -= learning_rate * gradient

print(f"Vector w final: {w}")

# Cerrar sesión de Spark
spark.stop()
