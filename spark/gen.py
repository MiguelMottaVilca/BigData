import random

# ejercicio 1
with open("log_data.txt", "w") as f:
    for i in range(1000):
        if random.random() < 0.1:
            f.write("ERROR: Ocurrió un error en la operación.\n")
        else:
            f.write("INFO: Operación completada exitosamente.\n")


# ejercicio 2
D = 10  # Dimensión del espacio de características
n_points = 1000  # Número de puntos de datos

with open("logistic_data.txt", "w") as f:
    for _ in range(n_points):
        x = [random.uniform(-1, 1) for _ in range(D)]  # Genera valores aleatorios entre -1 y 1
        y = random.choice([-1, 1])  # Etiqueta aleatoria
        line = ",".join(map(str, x)) + f",{y}\n"  # Formato: x1,x2,...,xD,y
        f.write(line)

# ejercicio 3
n_users = 100  # Número de usuarios
n_movies = 50  # Número de películas

with open("ratings_data.txt", "w") as f:
    for user in range(n_users):
        for movie in range(n_movies):
            if random.random() < 0.2:  # 20% de probabilidades de que haya una calificación
                rating = random.randint(1, 5)  # Calificación entre 1 y 5
                f.write(f"{user},{movie},{rating}\n")
