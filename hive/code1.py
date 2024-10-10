import random

# Generar líneas de texto aleatorias
words = ["hadoop", "hive", "mapreduce", "data", "analytics", "python", "java", "sql", "bash"]
lines = [" ".join(random.choices(words, k=random.randint(5, 15))) for _ in range(100)]

# Guardar en un archivo
with open('wordcount_log.txt', 'w') as file:
    file.write("\n".join(lines))

print("Archivo 'wordcount_log.txt' generado.")