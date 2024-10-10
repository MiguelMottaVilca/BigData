import random

# Definir lista de usuarios
users = [f"user{str(i).zfill(3)}" for i in range(1, 21)]
pages = [f"page{str(i).zfill(2)}" for i in range(1, 16)]

# Generar registros de rankeo de páginas
with open('page_rank.txt', 'w') as file:
    for _ in range(200):
        user = random.choice(users)
        page = random.choice(pages)
        rank = round(random.uniform(0, 1), 2)
        file.write(f"{user}\t{page}\t{rank}\n")

print("Archivo 'page_rank.txt' generado.")
