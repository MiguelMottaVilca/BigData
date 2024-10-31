from pyspark.sql import SparkSession

# Iniciar sesión de Spark
spark = SparkSession.builder.appName("ALS").getOrCreate()

# Cargar la matriz de calificaciones en RDD y transmitirla
R = spark.read.text("s3://ruta_a_tu_archivo").rdd.map(lambda r: r[0].split(',')).collect()
Rb = spark.sparkContext.broadcast(R)

# Definir dimensiones
num_users = 100  # Número de usuarios
num_movies = 50  # Número de películas
D = 10  # Dimensión de las características

# Inicializar las matrices de usuarios y películas
U = np.random.rand(num_users, D)
M = np.random.rand(num_movies, D)

# Número de iteraciones
ITERATIONS = 10

# Función de actualización para usuarios
def update_user(user_index, Rb, M):
    # Actualización específica de usuario usando Rb y M
    # (para simplificación, se utiliza un promedio)
    user_ratings = np.array(Rb.value[user_index])
    U[user_index] = np.dot(user_ratings, M) / (np.linalg.norm(M) + 1e-9)
    return U[user_index]

# Función de actualización para películas
def update_movie(movie_index, Rb, U):
    # Actualización específica de película usando Rb y U
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
