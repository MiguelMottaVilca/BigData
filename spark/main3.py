import numpy as np
from pyspark.sql import SparkSession

# Iniciar sesión de Spark
spark = SparkSession.builder.appName("ALS").getOrCreate()

# Función para parsear cada línea del archivo a una entrada de la matriz
def parse_point(line):
    values = line.split(',')
    user_id = int(values[0])
    movie_id = int(values[1])
    rating = float(values[2])
    return (user_id, movie_id, rating)

# Cargar los datos desde el archivo y construir la matriz de calificaciones
ratings = spark.read.text("hdfs://ip-172-31-92-76.ec2.internal:8020/user/hadoop/ratings_data.txt").rdd.map(lambda r: parse_point(r[0]))

num_users = 100
num_movies = 50
R = np.zeros((num_users, num_movies))

# Llenar la matriz con las calificaciones existentes
for user_id, movie_id, rating in ratings.collect():
    R[user_id, movie_id] = rating

# Transmitir la matriz de calificaciones
Rb = spark.sparkContext.broadcast(R)

# Definir dimensiones
D = 10  # Dimensión de las características

# Inicializar las matrices de usuarios y películas
U = np.random.rand(num_users, D)
M = np.random.rand(num_movies, D)

# Número de iteraciones
ITERATIONS = 10

# Función de actualización para usuarios
def update_user(user_index, Rb, M):
    user_ratings = np.array(Rb.value[user_index])
    U[user_index] = np.dot(user_ratings, M) / (np.linalg.norm(M) + 1e-9)
    return U[user_index]

# Función de actualización para películas
def update_movie(movie_index, Rb, U):
    movie_ratings = np.array([Rb.value[user][movie_index] for user in range(num_users)])
    M[movie_index] = np.dot(movie_ratings, U) / (np.linalg.norm(U) + 1e-9)
    return M[movie_index]

# Ejecutar iteraciones de ALS
for i in range(ITERATIONS):
    # Actualizar usuarios
    U = spark.sparkContext.parallelize(range(num_users)).map(lambda j: update_user(j, Rb, M)).collect()
    
    # Actualizar películas
    M = spark.sparkContext.parallelize(range(num_movies)).map(lambda j: update_movie(j, Rb, U)).collect()

print("Matriz de usuarios U:", U)
print("Matriz de películas M:", M)

# Cerrar sesión de Spark
spark.stop()
